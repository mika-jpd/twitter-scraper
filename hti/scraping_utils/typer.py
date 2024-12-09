import asyncio.exceptions
from my_utils.logger.logger import logger
from scipy.stats import multivariate_normal
import random
import asyncio


class Typer:
    qwertyKeyboardArray = [
        ['`', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '-', '='],
        ['q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', '[', ']', '\\'],
        ['a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ';', '\'', '\n'],
        ['z', 'x', 'c', 'v', 'b', 'n', 'm', ',', '.', '/'],
        [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ']
    ]

    qwertyShiftedKeyboardArray = [
        ['~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '+'],
        ['Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', '{', '}', '|'],
        ['A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', ':', '"'],
        ['Z', 'X', 'C', 'V', 'B', 'N', 'M', '<', '>', '?'],
        [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ']
    ]

    def __init__(self, accuracy=0.9, correction_chance=0.6, typing_delay=(0.04, 0.11), distance=1):
        if type(typing_delay) in [int, float]:
            self.delayMode = "strict"
        elif type(typing_delay) in [list, tuple]:
            if len(typing_delay) == 2:
                self.delayMode = "random"
            else:
                self.WrongArgumentException()
        else:
            self.WrongArgumentException()
        self.dist = distance
        self.delay = typing_delay
        self.acc = accuracy
        self.correction = correction_chance

    async def getDelay(self):
        if self.delayMode == "strict":
            return self.delay
        else:
            return random.random() * (self.delay[1] - self.delay[0]) + self.delay[0]

    def WrongArgumentException(self):
        raise Exception("Wrong Argument Exception")

    async def send(self, text: str, tab: Tab, value: str):
        isError = False
        true = ""
        for a in text:
            if random.random() > self.acc:
                # make error
                cast = self.wrongCharacterChoice(a, dist=self.dist)
                isError = True
            else:
                cast = a
            element: Element = await tab.select(value)
            # send a character and wait according to typing delay
            await element.send_keys(cast)
            await asyncio.sleep(await self.getDelay())

            # record true values for errors made
            if isError:
                true += a

            if random.random() < self.correction and isError:
                # fix error
                await self.sendTextOneByOne(text=true, tab=tab, value=value, send="recursive")
                await asyncio.sleep(await self.getDelay())
                isError = False
                true = ""

        # type all remaining text
        if isError:
            element: Element = await tab.select(value)
            await self.sendTextOneByOne(text=true, tab=tab, value=value)

    async def sendTextOneByOne(self, text, tab: Tab, value, send="all"):
        for _ in range(len(text)):
            element: Element = await tab.select(value)
            await element.send_keys('\ue0008')  # how to send a backspace?
            await asyncio.sleep(await self.getDelay() / 4)
        if send == "all":
            for k in text:
                await element.send_keys(k)
                await asyncio.sleep(await self.getDelay())
        elif send == "recursive":
            await self.send(text, tab, value)

    @staticmethod
    def getProb(key, neighbor):
        var = multivariate_normal(mean=key, cov=[[1, 0], [0, 1]])
        return var.pdf(neighbor)

    def getTuple(self, char):
        k = [(index, row.index(char)) for index, row in enumerate(self.qwertyKeyboardArray) if char in row]
        arr = self.qwertyKeyboardArray
        if len(k) == 0:
            k = [(index, row.index(char)) for index, row in enumerate(self.qwertyShiftedKeyboardArray) if char in row]
            arr = self.qwertyShiftedKeyboardArray
        if len(k) == 0:
            print("Please provide English text only")
            return (4, 0), arr
        return k[0], arr

    @staticmethod
    def getChar(tup, arr):
        return arr[tup[0]][tup[1]]

    @staticmethod
    def getAllNeighbors(tup, arr, dist):
        bounds = [len(i) - 1 for i in arr]
        xs = []
        ys = []
        tups = []
        r = list(range(dist + 1))
        r += [-1 * k for k in r if not k == 0]
        for i in r:
            val = tup[0] + i
            if 4 >= val >= 0:
                xs.append(val)
            val = tup[1] + i
            ys.append(val)
        for k in range(len(xs)):
            tups += [(xs[k], ys[i]) for i in range(len(ys)) if bounds[xs[k]] >= ys[i] >= 0]
        return tups

    @staticmethod
    def wrongCharacterChoice(char, dist=1):
        tup, arr = Typer.getTuple(Typer, char)
        j = [a for a in Typer.getAllNeighbors(tup, arr, dist) if not a == tup]
        probs = [Typer.getProb(tup, i) for i in j]
        ans = random.choices(j, probs, k=1)[0]
        result = Typer.getChar(ans, arr)
        if arr == Typer.qwertyShiftedKeyboardArray:
            try:
                new = Typer.getChar(ans, Typer.qwertyKeyboardArray)
                return random.choice([result, new])
            except Exception as e:
                pass
        return result
