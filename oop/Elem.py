class Elem:
    def __init__(self,name,filename):
        self.name= name
        self.filename = filename

    def __str__(self):
        if self.type == "JOBS":
            return f'{self.name}|| {self.type} || {self.filename}'
        else:
            return f'{self.name} || {self.type}'

    def getFilenames(self,filenames):
        return f'{self.filename}'

