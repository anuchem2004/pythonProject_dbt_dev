class  Job:
    def __init__(self,name="", filename=None):
        self.name = name
        self.type = type
        self.tasklist = []

        self.jobList = []
        self.parameters ={}

    def appendJobList(self,job):
        self.jobList.append(job)
        return

    def appendTaskList(self,element):
        self.tasklist.append(element)

    def setFileName(self,filename):
        self.filename = filename

    def setName(self, name):
        self.name = name

    def getName(self):
        return str(self.name)
