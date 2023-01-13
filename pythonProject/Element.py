class Element:
    def __int__(self,name,type,value=None,filename=None):
        self.name=name
        self.type=type
        self.value=value
        self.filename=filename


    def __str__(self):
        if self.type =='JOB':
            return f"{self.name} || {self.type} || {self.filename}"
        else:
            return f"{self.name} || {self.type}"

    def setFileName(self,filename):
        self.filename = filename
