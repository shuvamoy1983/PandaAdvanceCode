class Student(object):
    "Common base for all members."

    def __init__(self, val1, val2):
        self.val1 = val1
        self.val2 = val2

    def add(self):
        c = self.val1 + self.val2
        return c