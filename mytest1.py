

from mytest import Student

class Graduates(Student):
    def __init__(self, val1, val2, val4):
        Student.__init__(self, val1, val2)
        self.val4 = val4

    def calc(self):
        avg = Student.add(self)/self.val4
        return avg



v = Graduates(10,20,30)
print(v.calc())




