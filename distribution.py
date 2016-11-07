# Create a distribution of neantigens and neoepitopes

import sys, os
from collections import Counter
# import numpy as np
# import scipy.stats as stats
# import pylab as pl

EPITOPE_LENGTH = 4
NUM_PATIENT_CUTOFF = 1

def read_labels(path):

	fp = open(path, 'r')
	fp.readline()
	lines = fp.readlines()
	fp.close()

	labels = {}
	label = 0
	for line in lines:
		cols = line.split(',')
		pat_id = cols[0]
		label_col = cols[7]
		
		if not label_col == "nonresponse":
			label = 1
		else:
			label = 0

		labels[pat_id] = label
 
	return labels


def read_csv(path):

	fp = open(path, 'r')
	data = fp.readline()
	fp.close()

	data = data.split('Pat')[1:]

	patients = {}
	for i, line in enumerate(data):
		# if i > 1000: break
		cols = line.rstrip('\n').split(',')
		try:
			pat_id = cols[0]
			neoantigen = cols[10]
		except Exception:
			# print "ERROR: reading " + line
			continue
		# print 'INFO: ' + pat_id + ' ' + neoantigen

		if (':') in neoantigen:
			continue

		if patients.has_key(pat_id):
			patients[pat_id].append(neoantigen)
		else:
			patients[pat_id] = [neoantigen]


	return patients

def compute_distribution(patients):

	counts = []
	for patient in patients:
		count = int(len(patients[patient]))
		counts.append(count)
		# print "Num antigens for " + patient + ": " + str(count)

	print counts

	# fit = stats.norm.pdf(counts, np.mean(counts), np.std(counts))
	# pl.plot(counts,fit,'-o')

	pl.hist(counts,normed=True)
	pl.show()

def extract_neoepitopes(patients):

	neoepitope_dict = {}
	for patient in patients:
		neoantigens = patients[patient]
		neoepitopes = []
		for neoantigen in neoantigens:
			neoepitopes.extend(get_all_substrings(neoantigen))

		for neoepitope in neoepitopes:
			if neoepitope_dict.has_key(neoepitope):
				neoepitope_dict[neoepitope].append(patient)
			else:
				neoepitope_dict[neoepitope] = [patient]

	return neoepitope_dict


def get_all_substrings(input_string):
  length = len(input_string)
  options = [input_string[i:j+1] for i in xrange(length) for j in xrange(i,length)]
  tetramers = []
  for option in options:
  	if len(option) == EPITOPE_LENGTH:
  		tetramers.append(option)
  return tetramers


def find_min_error(counters):
	
	if len(counters) == 1:
		return 1

	errors = []
	split = 1
	while split < len(counters):
		first = counters[:split]
		second = counters[split:]
		
		num_ones_in_first = 0
		num_zeros_in_second = 0
		for item in first:
			if item[2] == 1:
				num_ones_in_first += 1
		for item in second:
			if item[2] == 0:
				num_zeros_in_second += 1

		error1 = 1 - (float(num_ones_in_first) / len(first))
		error2 = 1 - (float(num_zeros_in_second) / len(second))
		avg_error = (error1 + error2) / 2.0;
		errors.append(avg_error)

		split += 1

	return min(errors)


def find_most_informative(label_dict, neoepitope_dict):

	results = []

	for neoepitope in neoepitope_dict:

		c = Counter(neoepitope_dict[neoepitope])
		counters = c.items()

		for i,pair in enumerate(counters):
			counters[i] =(pair[0], pair[1], label_dict['Pat' + pair[0]])

		counters = sorted(counters, key=lambda x: x[1], reverse=True)

		error = find_min_error(counters)
		results.append( (neoepitope,error,len(counters)) )
 
	results = sorted(results,key=lambda x:x[2],reverse=True)
	results = sorted(results,key=lambda x:x[1])

	most_informative_epitopes = []
	for result in results:
		if (result[1] == 0) and (result[2] > NUM_PATIENT_CUTOFF):
			# print '{}|{}|{}'.format(result[0],result[1],result[2])
			most_informative_epitopes.append(result[0])

	return most_informative_epitopes



def main():
	input_path = "/Users/allipine/Desktop/ResearchML/Data/Neoantigens_All2.csv"
	label_path = "/Users/allipine/Desktop/ResearchML/Data/LabelSet_All.csv"
	patients = read_csv(input_path)
	label_dict = read_labels(label_path)

	neoepitope_dict = extract_neoepitopes(patients)

	most_informative_epitopes = find_most_informative(label_dict, neoepitope_dict)

	for epitope in most_informative_epitopes:
		print "{},".format(epitope),

	# compute_distribution(patients)


if __name__ == "__main__":
	main()





