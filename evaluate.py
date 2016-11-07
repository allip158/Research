
import sys, os
from sklearn.metrics import classification_report

def read_csv(path):
	
	fp = open(path, 'r')
	lines = fp.readlines()
	fp.close()

	labels = {}
	predictions = {}
	for line in lines:
		line = line.replace(' ' , '')
		cols = line.rstrip('\n').split(',')
		if cols[1] == 'labels':
			key = cols[0]
			value = cols[2:]
			labels[key] = value
		elif cols[1] == 'predictions':
			key = cols[0]
			value = cols[2:]
			predictions[key] = value

	return labels, predictions	

def compute_metrics(labels, predictions):

	for key in labels:
		actual = labels[key]
		preds = predictions[key]

		target_names = ['Nonresponder', 'Responder']
		print "Run " + key
		print(classification_report(actual, preds, target_names=target_names))



def main():
	input_path = "/Users/allipine/Desktop/ResearchML/Output/output_snyder_wildtype_epitopecluster.csv"
	labels, predictions = read_csv(input_path)
	compute_metrics(labels, predictions)

if __name__ == "__main__":
	main()