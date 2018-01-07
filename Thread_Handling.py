import sys

import os
from threading import Thread
from time import sleep


class myClass():

    def help(self):
        os.system('python /Users/shuvamoymondal/PycharmProjects/pythonTutorial/FileHandling.py')

    def nope(self):
        a = [1,2,3,4,5,6,67,78]
        for i in a:
            print(i)
            sleep(1)


if __name__ == "__main__":
    Yep = myClass()
    thread = Thread(target = Yep.help)
    thread2 = Thread(target = Yep.nope)
    thread.start()
    thread2.start()
    thread.join()
    print('Finished')