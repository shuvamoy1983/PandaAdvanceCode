#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 22:54:47 2017

@author: shuvamoymondal
"""

##Method used in python data structures:
myList = [1024, 3, True, 6.5]
print(myList)

##if you want to append a value to the list

myList.append(False)
print(myList)

##Inserts an item at the ith position in a list
##alist.insert(i,item)

myList.insert(2, 2)
print(myList)

###Removes and returns the last item in a list : pop()
myList.pop()
print(myList)

##Removes and returns the ith item in a list
myList.pop(2)
print(myList)

##alist.sort()	Modifies a list to be sorted

myList.sort()
print(myList)

##Modifies a list to be in reverse order or print(myList[::-1])
myList.reverse()
print(myList)

##Deletes the item in the ith position
del myList[3]
print(myList)

##Returns the index of the first occurrence of item
print(myList.index(1024))

##alist.count(item)	Returns the number of occurrences of item

aList = [123, 'xyz', 'zara', 'abc', 123];

print("Count for 123 : ", aList.count(123))
print("Count for zara : ", aList.count('zara'))

##The method extend() appends the contents of seq to list

aList = [123, 'xyz', 'zara', 'abc', 123];
bList = [2009, 'manni'];
aList.extend(bList)

print("Extended List : ", aList)
########  in built Function to be used ################3

##Searching For Values In A List
a_list = ['a', 'b', 'new', 'mpilgrim', 'j']
print(a_list.count('new'))
print(a_list[1:3])
print(a_list[0:3])
print(a_list[1:-1])

# But what's the difference between lists and dictionaries?
# A list is an ordered sequence of objects, whereas dictionaries are unordered sets.
# But the main difference is that items in dictionaries are accessed via
# keys and not via their position.


dict = {'Name': 'Zara', 'Age': 7, 'Class': 'First'}
print("dict['Name']: ", dict['Name'])

###pop() is defined differently with dictionaries
capitals = {"Austria": "Vienna", "Germany": "Berlin", "Netherlands": "Amsterdam"}
capital = capitals.pop("Austria")
print(capital) ## It will print Vienna

##popitem() is a method of dict, which doesn't take any parameter and removes and returns an arbitrary (key,value) pair as a 2-tuple.
##If popitem() is applied on an empty dictionary, a KeyError will be raised

capitals = {"Springfield": "Illinois", "Augusta": "Maine",
            "Boston": "Massachusetts", "Lansing": "Michigan",
            "Albany": "New York", "Olympia": "Washington", "Toronto": "Ontario"}

(city, state) = capitals.popitem()
print((city, state))

## It will print city and state together;


trainings = {"course1": {"title": "Python Training Course for Beginners",
                         "location": "Frankfurt",
                         "trainer": "Steve G. Snake"},
             "course2": {"title": "Intermediate Python Training",
                         "location": "Berlin",
                         "trainer": "Ella M. Charming"},
             "course3": {"title": "Python Text Processing Course",
                         "location": "MÃ¼nchen",
                         "trainer": "Monica A. Snowdon"}
             }

tr2 = trainings.copy()
print(tr2)

print(trainings["course2"]["title"])

##Merging Dictionaries

knowledge = {"Frank": {"Perl"}, "Monica": {"C", "C++"}}
knowledge2 = {"Guido": {"Python"}, "Frank": {"Perl", "Python"}}

knowledge.update(knowledge2)
print(knowledge)

d = {"a": 123, "b": 34, "c": 304, "d": 99}
for key in d:
    print(key)
for value in d.values():
    print(value)

##Lists from Dictionaries

w = {"house": "Haus", "cat": "", "red": "rot"}
items_view = w.items()
print(items_view)

keys_view = w.keys()
keys = list(keys_view)
print(keys)

value_view = w.values()
value = list(value_view)
print(value)
print(value_view)

##Turn Lists into Dictionaries

dishes = ["pizza", "sauerkraut", "paella", "hamburger"]
countries = ["Italy", "Germany", "Spain", "USA"]

mydict = {}
for i, j in zip(countries, dishes):
    mydict[i] = j
print(mydict)

l1 = ["a", "b", "c"]
l2 = [1, 2, 3]
c = zip(l1, l2)
z1 = list(c)
z2 = list(c)

print(z1)
print(z2)

import turtle


def draw_square(t, sz):
    """Make turtle t draw a square of sz."""
    for i in range(4):
        t.forward(sz)
        t.left(90)


wn = turtle.Screen()  # Set up the window and its attributes
wn.bgcolor("lightgreen")
wn.title("Alex meets a function")

alex = turtle.Turtle()  # Create alex
draw_square(alex, 50)  # Call the function to draw the square
wn.mainloop()


