import pandas as pd

##broken_df = pd.read_csv('/Users/shuvamoymondal/Downloads/sample.csv')
fixed_df = pd.read_csv('/Users/shuvamoymondal/Downloads/sample.csv', sep=';', encoding='UTF8')
print(fixed_df[:3])

tt = {"a", "b", "c", \
      "f", "g", \
      "l"}
print(id(tt))

string_two = "He said:\"You have been nominated as honorary president of the Mouse Clickers Club\" " + "annknks"
print(string_two)

string_one = "program"
string_two = "is"
string_three = "worth watching"
print("An excellent " + string_one + " " + string_two + " " + string_three + ".")

smart_var = "Europe"
print(smart_var.upper())

print(string_three.find("r"))

b = "******A special symbol is supposed to introduce this string.*****"
print(b.lstrip('*'))


def greet(name):
    """Greets the person
     passed as argument"""
    print("Hello, " + name + ". Good day!")


print(greet.__doc__)


def class_sum(num):
    return num * 3


def school_sum(m):
    return class_sum(m) + 3


print(school_sum(5))
