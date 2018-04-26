import random
from operator import attrgetter


def generateEntities(entityNumber):
	entityLst = []

	for x in range(0, entityNumber):
		s = ""
		for ch in target:
			s += random.choice(alphabet)
		newEntity = Entity(s, 0)
		entityLst.append(newEntity)
	
	return entityLst

def reproduce(entities, reproduceN):
	newEntityLst = []

	for x in range(0, reproduceN):
		first = entities[1]
		second = entities[random.randint(0, len(entities) / 2)]

		# choose genetic line
		line = random.randint(1, len(target) - 1)
		output = ""

		# combine entity information
		for it in range(0, line):
			if (random.randint(0, 101) < mutation_chance):
				output += random.choice(alphabet)
			else:
				output += first.output[it]

		for it in range(line, len(target)):
			if (random.randint(0, 101) < mutation_chance):
				output += random.choice(alphabet)
			else:
				output += second.output[it]

		newEntity = Entity(output, 0)
		newEntityLst.append(newEntity)

	entities.extend(newEntityLst)
	return entities


class Entity:
	output = ""
	fitness = 0

	def __init__(self, output, fitness):
		self.output = output
		self.fitness = fitness

	def setFitness(self, target):
		right = 0
		# compare strings character by character and determine fitness
		for c1, c2 in zip(target, self.output):
			if (c1 == c2):
				right = right + 1

		self.fitness = right / float(len(target))
		#print self.fitness

#############################################################################################


alphabet = "abcdefghijklmnopqrstuvwxyz !ABCDEFGHIJKLMNOPQRSTUVWXYZ"
target = "Hello World!"
mutation_chance = 20
init_population = 500

entities = generateEntities(init_population)

#for it in entities:
#	print it.output

generation = 0
bestFitness = -1

while (max((o.fitness) for o in entities) != 1):
	generation = generation + 1

	# set fitness for all
	map(lambda x: x.setFitness(target), entities)

	#for it in entities:
	#	print it.output, it.fitness

	#a = raw_input("pause")

	# order by fitness
	entities.sort(key = lambda x: x.fitness, reverse = True)

	# kill worst half
	kill = init_population / 5
	entities = entities[0:kill]

	# generate again
	entities = reproduce(entities, kill)

	bestFitness = max((o.fitness) for o in entities)
	
	print "Generation: ", generation
	print "Best fitness: ", bestFitness
	print "Output: ", max(entities, key = attrgetter('fitness')).output

# end




		