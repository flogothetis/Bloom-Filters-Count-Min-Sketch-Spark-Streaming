import random
#This programm writes random ip adresses to a file
file = open("ip_adresses_file.txt", "w");

for i in range (1,50000):
	file.write(str(random.randint(100000000000,999999999999)));
	file.write("\n")
file.close()