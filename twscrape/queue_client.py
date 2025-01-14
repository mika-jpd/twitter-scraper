import json
import os
from typing import Any
import numpy as np
import datetime
import random

import httpx
from httpx import AsyncClient, Response

from .accounts_pool import Account, AccountsPool
from .logger import logger
from .utils import utc

ReqParams = dict[str, str | int] | None
TMP_TS = utc.now().isoformat().split(".")[0].replace("T", "_").replace(":", "-")[0:16]

# added by mika_jpd
from .models import parse_tweets, parse_tweet, parse_users, parse_user


class Ctx:
    def __init__(self, acc: Account, clt: AsyncClient):
        self.acc = acc
        self.clt = clt
        self.req_count = 0


class HandledError(Exception):
    pass


class AbortReqError(Exception):
    pass


def req_id(rep: Response):
    lr = str(rep.headers.get("x-rate-limit-remaining", -1))
    ll = str(rep.headers.get("x-rate-limit-limit", -1))
    sz = max(len(lr), len(ll))
    lr, ll = lr.rjust(sz), ll.rjust(sz)

    username = getattr(rep, "__username", "<UNKNOWN>")
    return f"{lr}/{ll} - {username}"


def dump_rep(rep: Response):
    count = getattr(dump_rep, "__count", -1) + 1
    setattr(dump_rep, "__count", count)

    acc = getattr(rep, "__username", "<unknown>")
    outfile = f"{count:05d}_{rep.status_code}_{acc}.txt"
    outfile = f"/tmp/twscrape-{TMP_TS}/{outfile}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    msg = []
    msg.append(f"{count:,d} - {req_id(rep)}")
    msg.append(f"{rep.status_code} {rep.request.method} {rep.request.url}")
    msg.append("\n")
    # msg.append("\n".join([str(x) for x in list(rep.request.headers.items())]))
    msg.append("\n".join([str(x) for x in list(rep.headers.items())]))
    msg.append("\n")

    try:
        msg.append(json.dumps(rep.json(), indent=2))
    except json.JSONDecodeError:
        msg.append(rep.text)

    txt = "\n".join(msg)
    with open(outfile, "w") as f:
        f.write(txt)


class QueueClient:
    def __init__(self, pool: AccountsPool, queue: str, debug=False, proxy: str | None = None, use_case: int = None,
                 _num_calls_before_humanization: tuple[int, int] = (15, 30)):
        self.pool = pool
        self.queue = queue
        self.debug = debug
        self.ctx: Ctx | None = None
        self.proxy = proxy

        # added by mika_jpd
        self.use_case = use_case
        self._num_calls_before_humanization: tuple[int, int] = _num_calls_before_humanization

    async def __aenter__(self):
        await self._get_ctx() #Todo: double check if this is actually good.
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._close_ctx()

    async def _close_ctx(self, reset_at=-1, inactive=False, msg: str | None = None, login_again: bool = False):
        if self.ctx is None:
            return

        ctx, self.ctx, self.req_count = self.ctx, None, 0
        username = ctx.acc.username
        await ctx.clt.aclose()

        await self.pool.set_in_use(username=username, in_use=False)

        if (login_again) and (not ctx.acc.last_login == -2) and (not inactive): # for the case where you're sending the account after a request error
            login_status: int = 0
            for i in range(2):
                login_status = await self.pool.humanize_account(account=ctx.acc, queue="_close_ctx")
                if login_status != -2:
                    break
            await self.pool.set_last_login(username=username, last_login=login_status)
            if login_status != 1:
                err_msg: str = f"failed humanization with login_status {login_status}"
                logger.error(f"Marking {username} inactive with error_msg: {err_msg}")
                await self.pool.set_active(username=username, error_msg=err_msg, active=False)
            else:
                logger.warning(f"Marking {username} as active with login status after a second try: {login_status}")
                await self.pool.set_active(username=username, active=True)
                inactive = False

        if inactive:
            logger.error(f"Marking {username} inactive with error_msg: {msg}")
            await self.pool.set_active(username=username, active=False, error_msg=msg)
            return

        if reset_at > 0:
            logger.warning(f"Resetting {ctx.acc.username}'s lock until {reset_at}")
            await self.pool.lock_until(username, self.queue, reset_at, ctx.req_count)
        else:
            await self.pool.unlock(username, self.queue, ctx.req_count)

    async def _get_ctx(self):

        if self.ctx:
            acc = await self.pool.get_for_queue_or_wait(
                queue=self.queue, username=self.ctx.acc.username,
                _num_calls_before_humanization=self._num_calls_before_humanization,
                use_case=self.use_case
            )  # get the same account
            if acc:
                self.ctx.acc = acc
                return self.ctx
            else:
                await self._close_ctx(login_again=True)

        # self.ctx is not given an account yet
        acc = await self.pool.get_for_queue_or_wait(
            queue=self.queue, use_case=self.use_case,
            _num_calls_before_humanization=self._num_calls_before_humanization
        )  # attach new account to self.ctx.account
        if acc is None:
            return None

        clt = acc.make_client(proxy=self.proxy)
        self.ctx = Ctx(acc, clt)
        return self.ctx

    async def _check_rep(self, rep: Response) -> None:
        """
        This function can raise Exception and request will be retried or aborted
        Or if None is returned, response will passed to api parser as is
        """

        if self.debug:
            dump_rep(rep)

        try:
            res = rep.json()
        except json.JSONDecodeError:
            res: Any = {"_raw": rep.text}

        limit_remaining = int(rep.headers.get("x-rate-limit-remaining", -1))
        limit_reset = int(rep.headers.get("x-rate-limit-reset", -1))
        # limit_max = int(rep.headers.get("x-rate-limit-limit", -1))

        err_msg = "OK"
        if "errors" in res:
            err_msg = set([f'({x.get("code", -1)}) {x["message"]}' for x in res["errors"]])
            err_msg = "; ".join(list(err_msg))

        min_date: str | None = None
        max_date: str | None = None
        top_users: str | None = None
        total_tweets: int | None = None
        user_id = None
        username = None
        screen_name = None

        try:
            if self.queue in ['SearchTimeline', 'UserTweets', 'UserTweetsAndReplies'] and rep.status_code == 200: # parse tweets
                tweets = [i for i in parse_tweets(rep, -1)]

                # remove the original quoted and RT tweets
                rt_tweets = [t.retweetedTweet.id for t in tweets if t.retweetedTweet]
                qt_tweets = [t.quotedTweet.id for t in tweets if t.quotedTweet]
                tweets = [t for t in tweets if (t.id not in rt_tweets) and (t.id not in qt_tweets)]

                # remove pinned tweet
                if len(tweets) > 1:
                    pinned_tweets = random.sample(tweets[1:], 1).pop().user.pinnedIds
                    tweets = [t for t in tweets if t.id not in pinned_tweets]

                # fetch the dates and users
                dates = [i.date for i in tweets]
                users = [i.user.username for i in tweets]

                # fetch some stats
                total_tweets = len(tweets)
                min_date = min(dates).strftime("%d/%m/%Y-%H:%M:%S")
                max_date = max(dates).strftime("%d/%m/%Y-%H:%M:%S")
                top_user_values, top_user_count = np.unique(users, return_counts=True)
                top_users = "".join([f"{v} ({c}) " for v, c in zip(top_user_values, top_user_count)]).rstrip()
            elif self.queue in ["UserByScreenName"] and rep.status_code == 200:
                user = parse_user(rep)
                user_id = user.id_str
                username = user.username
                screen_name = user.displayname
        except Exception as e:
            logger.warning(f"Error {e} while parsing with bot account {self.ctx.acc.username} for queue: {self.queue}")
        if self.queue in ['SearchTimeline', 'UserTweets', 'UserTweetsAndReplies']:
            msg = f"Collected {total_tweets} tweets from {top_users} between {min_date} & {max_date}"
        elif self.queue in ['UserByScreenName']:
            msg = f"Fetched user {username} with ID {user_id} and screename {screen_name}"
        else:
            msg = "Unknown queue"
        log_msg = (f"{rep.status_code:3d} - {req_id(rep)} - {err_msg} - " + msg).rstrip()

        logger.info(log_msg)

        # for dev: need to add some features in api.py
        if err_msg.startswith("(336) The following features cannot be null"):
            logger.error(f"[DEV] Update required: {err_msg}")
            exit(1)

        # general api rate limit
        if limit_remaining <= 5 and limit_reset > 0:
            limit_rests_strftime = datetime.datetime.fromtimestamp(limit_reset).strftime("%m/%d/%Y, %H:%M:%S")
            logger.debug(f"Rate limited: {log_msg} until {limit_rests_strftime}")
            await self._close_ctx(limit_reset)
            raise HandledError()

        # no way to check is account banned in direct way, but this check should work
        if err_msg.startswith("(88) Rate limit exceeded") and limit_remaining > 0:
            logger.warning(f"Ban detected: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg.startswith("(326) Authorization: Denied by access control"):
            logger.warning(f"Ban detected: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg.startswith("(32) Could not authenticate you"):
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=err_msg)
            raise HandledError()

        if err_msg == "OK" and rep.status_code == 403:
            logger.warning(f"Session expired or banned: {log_msg}")
            await self._close_ctx(-1, inactive=True, msg=None, login_again=True)
            raise HandledError()

        # something from twitter side - abort all queries, see: https://github.com/vladkens/twscrape/pull/80
        if err_msg.startswith("(131) Dependency: Internal error"):
            # looks like when data exists, we can ignore this error
            # https://github.com/vladkens/twscrape/issues/166
            if rep.status_code == 200 and "data" in res and "user" in res["data"]:
                err_msg = "OK"
            else:
                logger.warning(f"Dependency error (request skipped): {err_msg}")
                raise AbortReqError()

        # content not found
        if rep.status_code == 200 and "_Missing: No status found with that ID" in err_msg:
            return  # ignore this error

        # something from twitter side - just ignore it, see: https://github.com/vladkens/twscrape/pull/95
        if rep.status_code == 200 and "Authorization" in err_msg:
            logger.warning(f"Authorization unknown error: {log_msg}")
            return

        if err_msg != "OK":
            logger.warning(f"API unknown error: {log_msg}")
            return  # ignore any other unknown errors

        try:
            rep.raise_for_status()
        except httpx.HTTPStatusError:
            logger.error(f"Unhandled API response code: {log_msg}")
            await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
            raise HandledError()

    async def get(self, url: str, params: ReqParams = None):
        return await self.req("GET", url, params=params)

    async def req(self, method: str, url: str, params: ReqParams = None) -> Response | None:
        unknown_retry, connection_retry = 0, 0

        while True:
            ctx = await self._get_ctx()  # not need to close client, class implements __aexit__
            if ctx is None:
                return None

            try:
                rep = await ctx.clt.request(method, url, params=params)
                setattr(rep, "__username", ctx.acc.username)
                await self._check_rep(rep)

                ctx.req_count += 1  # count only successful
                unknown_retry, connection_retry = 0, 0
                return rep
            except AbortReqError:
                # abort all queries
                return
            except HandledError:
                # retry with new account
                continue
            except (httpx.ReadTimeout, httpx.ProxyError):
                # http transport failed, just retry with same account
                continue
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                # if proxy missconfigured or ???
                connection_retry += 1
                if connection_retry >= 3:
                    raise e
            except Exception as e:
                unknown_retry += 1
                if unknown_retry >= 3:
                    msg = [
                        "Unknown error. Account timeouted for 15 minutes.",
                        "Create issue please: https://github.com/vladkens/twscrape/issues",
                        f"If it mistake, you can unlock accounts with `twscrape reset_locks`. Err: {type(e)}: {e}",
                    ]

                    logger.warning(" ".join(msg))
                    await self._close_ctx(utc.ts() + 60 * 15)  # 15 minutes
