import random, sys

fp = open('/Users/allipine/Desktop/ResearchML/Data/MutationFile.csv', 'r')
header = fp.readline()
lines = fp.readlines()
fp.close()

print "Number of lines: " + str(len(lines))

patients = {}
for line in lines:
	pid = line.split(',')[0]
	if patients.has_key(pid):
		patients[pid].append(line)
	else:
		patients[pid] = [line]

keys = list(patients.keys())
print "Number of patients: " + str(len(keys))
sys.exit(0)
random.shuffle(keys)


testing_lines = []
training_lines = []
for i,key in enumerate(keys):

	if i < 22:
		testing_lines.extend(patients[key])
	else:
		training_lines.extend(patients[key])

out_file = open('/Users/allipine/Desktop/ResearchML/Data/TrainingSet_Mutations_88.csv', 'w')
out_file.write(header)
for line in training_lines:
	out_file.write(line)
out_file.close()

out_file = open('/Users/allipine/Desktop/ResearchML/Data/ValidationSet_Mutations_22.csv', 'w')
out_file.write(header)
for line in testing_lines:
	out_file.write(line)
out_file.close()


print "Number of testing lines: " + str(len(testing_lines))
print "Number of training lines: " + str(len(training_lines))
