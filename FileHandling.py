import os

f = open("/Users/shuvamoymondal/Downloads/dept.txt",mode = 'r',encoding = 'utf-8')
try:
        v= f.read()
        with open("/Users/shuvamoymondal/Downloads/dept1.txt", mode='a') as g:
            for i in v:
              i #print(i)
              #g.write(i)
finally:
    f.close()

##/Users/shuvamoymondal/PycharmProjects/pythonTutorial




import zipfile

def read_zip_file(filepath):
    zfile = zipfile.ZipFile(filepath)
    for finfo in zfile.infolist():
        ifile = zfile.open(finfo)
        line_list = ifile.readlines()
        print(line_list)

if __name__ == '__main__':
    read_zip_file("/Users/shuvamoymondal/Downloads/apache-tomcat-8.5.23.zip")