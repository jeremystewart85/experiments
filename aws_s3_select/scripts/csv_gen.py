#!/usr/bin/python
import csv
import random
from faker import Factory

records = 10000

fieldnames=['id','name','age','address','currency',]
writer = csv.DictWriter(open("10k_people.csv", "w"), fieldnames=fieldnames)

#write header row
writer.writerow(dict(zip(fieldnames, fieldnames)))

fake = Factory.create("en_US")
for i in range(0, records):
	writer.writerow(dict([
	('id', str(random.randint(0,40000))),
	('name', fake.name()),
	('age', str(random.randint(18,120))),
	('address', fake.street_address()),
	('currency', fake.currency_code())]))
