from twscrape.api import API
import asyncio
import json

path_db = "/Users/mikad/MEOMcGill/twitter_scraper/accounts.db"
api = API(pool=path_db)

def process_cookies_out(cookies: list[dict], url: str | None = ".x.com") -> dict:
    out_cookies = {str(c["name"]): str(c["value"]) for c in cookies if url and "domain" in c and c["domain"] == url}
    return out_cookies

cookies = """{
 "cookieManagerVersion": "1.8",
 "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
 "cookies": [
 {
  "name": "guest_id_marketing",
  "value": "v1%3A173678339844074330",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1771343424,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "guest_id_ads",
  "value": "v1%3A173678339844074330",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1771343424,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "personalization_id",
  "value": "'v1_R2Ge/yBrKuwcJlaMo+HOhw=='",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1737388225,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "guest_id",
  "value": "v1%3A173678339844074330",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1768319399,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "night_mode",
  "value": "2",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1737388224,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "gt",
  "value": "1878831868608422262",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1736792399,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "kdt",
  "value": "k9lXPlwoh7FpFidIC3hmjbKXXo1ReSYME75mnO4U",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": true,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1771343423,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "twid",
  "value": "u%3D1818984910708719616",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1768319424,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "ct0",
  "value": "d298b365d8ed15db14a89cd18674d86a5eb1eab24c965fe832a6d28400e4d9d952afc6cc0503cf322ee97cc57f5b4de0859d353e599aad59de6b01523121316d3f499d38aed6d324c14084eb09d80872",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": false,
  "sameSite": "lax",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1771343424,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "auth_token",
  "value": "fc98bba3a1b498eaad6b626689ed2eb9c4887bed",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": true,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1771343423,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "att",
  "value": "1-w34hBBHgP3HTbOfkJOfg5hCSeKMtkOXTqawxpvmV",
  "domain": ".x.com",
  "hostOnly": false,
  "path": "/",
  "secure": true,
  "httpOnly": true,
  "sameSite": "no_restriction",
  "session": false,
  "firstPartyDomain": "",
  "partitionKey": null,
  "expirationDate": 1736869824,
  "storeId": "firefox-default",
  "url": "https://x.com/"
 },
 {
  "name": "lang",
  "value": "en",
  "domain": "x.com",
  "hostOnly": true,
  "path": "/",
  "secure": false,
  "httpOnly": false,
  "sameSite": "no_restriction",
  "session": true,
  "firstPartyDomain": "",
  "partitionKey": null,
  "storeId": "firefox-default",
  "url": "http://x.com/"
 }
]
}"""
cookies = json.loads(cookies)
cookies = cookies['cookies']
cookies = process_cookies_out(cookies)
username = 'mason_dian3965'

asyncio.run(
    api.pool.set_cookies(
        username=username,
        cookies=cookies
    )
)
