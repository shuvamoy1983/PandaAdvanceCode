Mark = {
           "Name": "Mark Spark",
           "Quizzes": [89.0, 95.0, 78.0, 90.0],
           "Homework": [89.0, 60.0, 98.0],
           "Recitation": [89.0,90.0, 88.0],
           "Tests": [85.0, 92.0]
}

Selen = {
"Name": "Selen Jobs",
"Quizzes": [98.0, 100.0, 95.0, 100.0],
"Homework": [85.0, 84.0, 90.0],
"Recitation": [87.0, 89.0, 90.0],
"Tests": [90.0, 97.0]
}


Shane = {
"Name": "Shane Taylor",
"Quizzes": [75.0, 87.0, 95.0, 84.0],
"Homework": [92.0, 74.0, 99.0],
"Recitation": [80.0, 83.0, 84.0],
"Tests": [98.0, 100.0]
}


def average(numbers):
    total=sum(numbers)
    result=total/len(numbers)
    return result


def get_average(student):
    Quizzes = average(student["Quizzes"])
    Homework = average(student["Homework"])
    Recitation = average(student["Recitation"])
    Tests = average(student["Tests"])
    print(student["Name"])
    return .2*Quizzes + .1*Homework + .3*Recitation + .4*Tests


print(get_average(Mark))
print(get_average(Shane))

squared = lambda x,y: (x +y)* 2
print(squared(10,10))


num_list = [1, 3, 5, 7, 9, 11, 8, 6, 4, 2]
v =list(map(lambda x: x *2,num_list))
print(v)


Num_lis = [12, 4, 1, 8, 9, 6, 11, 5, 2, 20]
odd_list = list(filter(lambda x: (x % 2 != 0) , Num_lis))
print(odd_list)


school_list = ["Biology", "English", "Chemistry", "Sociology", "Algebra"]
print(school_list[2][5])



nested_list = ["code", 4, [1, 3, 5, 7, 9]]
print("Nested list:" ,nested_list[2][4])

quick_list = ["s", "h", "o", "r", "t", "c", "u", "t"]
print(quick_list[0:4])


even = [1, 3, 5, 7, 9, 12]
even[1:5] = [4, 6, 8, 10]


print(even)

numbers=[8, 9, 6, 12, 8, 9, 10]
numbers[1:4] = (5, 6, 7)

print(numbers)

my_list = ["L", "I", "A", "B", "I", "L", "I", "T", "Y"]

del my_list[1:5]
print(my_list)



my_list = [1, 2, 3, 4, "O", "D", "D", 5, 6, 7, 8]
my_list[:] = []
print(my_list)


mm=(8, 9, 6, 12, 8, 9, 10)
print(mm[1:5:2])

my_tuple = ('a', 5, 3.5, ['P', 'y', 't', 'h', 'o', 'n'])
my_tuple[3][2]= 'x'
print(my_tuple)

print(enumerate(tuple({'Name':'Joshua', 'Animal':'elephant', 'Color': 'blue', 'Age':22} ) ))

lst=[1,2,3,4,5,56]
tup=(1,2,3,4,5,56)



#to see the method of list and tuple
print(dir(lst))
print(dir(tup))

import sys
print(dir(sys))
print("list space",sys.getsizeof(lst))
print("tup space",sys.getsizeof(tup))

my_dict = {'Name':'Mark', 'Age': 24, 'Ranking': '5th', 'Average':89.5}
my_dict['Status']='popular'
print(my_dict)

x = {3:"abc", 2:"def", 1:"ghi", 4:"jkl"}
y=x.copy()
print(y)

print("Sorted:" ,sorted(x))


print("val:",x.setdefault(2,'None'))
print(x.keys())
print(x.items())
print(x.values())

for n in x:
    print("dict:",n,x[n])