import random
import time

# to generate random number

nmber_dict_hum = {}
nmber_dict_temp = {}
nmber_temp = []
nmber_hum = []

class iotGenerator:

    def random_gen(self):
        while True:

            nmber = random.random()
            nmber_temp.append(nmber)
            nmber_hum.append(nmber)
            nmber_dict_hum['Humidty'] = nmber_hum
            nmber_dict_temp['Temperature'] = nmber_temp

            return nmber_dict_temp, nmber_dict_hum

    def hum_temp(self):

        while True:
            temp, hum = self.random_gen()
            print("The values for Humidity {} and Temperature is {}".format(hum, temp))
            time.sleep(10)


obj_gen = iotGenerator()

obj_gen.hum_temp()

# if __name__ == '__main__':
#     random_gen()

