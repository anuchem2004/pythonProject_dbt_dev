class Private_Object:
    def __init__(self,name="",type="",filename=""):
        self.name = name
        self.type= type
        self.filename =filename
        self.subjobList= ""
        self.subtransList = ""
        self.ChildList = ""
        self.Childfolder=""

    def printFile(self):
        print(f"filename is {self.filename} and name of file is {self.name}")

    def __str__(self,type):
        filepath= ["C:\\Users\\anuch\\Downloads\\pentaho_jobs_and_transformations\\"]
        if self.type == "JOBS":
            return f'{self.name}|| {self.type} || {self.filename}'
        else:
            return f'{self.name} || {self.type}'


# if __name__ == "__main__" :
#     # pr_obj = Private_Object("Anu","hello.ktt")
    # pr_obj.printFile()
    # print (pr_obj.name,pr_obj.filename)
