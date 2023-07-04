class Worker:
    def __init__(self, age, name, profession):
        self.age = age
        self.name = name
        self.profession = profession

    def get_data(self):
        print("Age multiplied by 2:", self.age * 2)
