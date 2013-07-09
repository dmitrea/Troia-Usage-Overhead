from datetime import datetime
import time
import re
from client.gal import TroiaClient
from nominalJobsTests.testSettings import *
import csv
import os
import sys

class ProcessData():

    APACK_SIZE = 500

    def loadAssigns(self, fileName):
        print 'Loading assigns file'
        assignedLabels = []
        assignsPack = []
        i = 0
        for line in open(fileName,'r'):
            i += 1
            rowData = re.split('\t', line)
            workerName = rowData[0]
            objectName = rowData[1]
            assignedLabel = rowData[2].strip('\n')
            label = (workerName, objectName, assignedLabel)
            assignsPack.append(label)
            if i == self.APACK_SIZE:
                assignedLabels.append(assignsPack)
                assignsPack = []
                i = 0
        assignedLabels.append(assignsPack)
        return assignedLabels


    def post_assigns(self, tc, assigns):
        initTime = datetime.now()
        i = 0
        for package in assigns:
            i += 1
            self.check_status(tc, tc.post_assigned_labels(package))
        loadAssigns_t = datetime.now() - initTime
        print loadAssigns_t
        return loadAssigns_t

    def loadCategories(self, fileName):
        print 'Loading categories file'
        categories = []
        for category in open(fileName, 'r'):
            categories.append(category.rstrip(os.linesep))
        return categories

    def loadGoldLabels(self, fileName):
        print 'Loading gold labels file'
        goldLabels = []
        try:
            for line in open(fileName, 'r'):
                rowData = re.split('\t', line)
                object = rowData[0]
                category = rowData[1].strip(os.linesep)
                goldLabels.append((object, category))

        except:
            print 'No golds'
        return goldLabels

    def loadEvaluationLabels(self, fileName):
        print 'Loading evaluation labels file'
        evaluationLabels = []
        try:
            for line in open(fileName,'r'):
                rowData = re.split('\t', line)
                object = rowData[0]
                category = rowData[1].strip(os.linesep)
                evaluationLabels.append((object, category))

        except:
            print 'No evaluation labels'
        return evaluationLabels

    def check_status(self, client, response):
        response = client.await_completion(response)
        if response['status'] == 'ERROR':
            print client.jid, response['result'], response.get('stacktrace')
            assert False
        return response

    def time_api_call(self, fun, *args, **kwargs):
        initTime = datetime.now()
        fun(*args, **kwargs)
        timeTaken = datetime.now() - initTime
        print timeTaken
        return timeTaken

    def time_data_load_call(self, fun, *args, **kwargs):
        initTime = datetime.now()
        data = fun(*args, **kwargs)
        timeTaken = datetime.now() - initTime
        print timeTaken
        return data

if __name__ == '__main__':
    dataSetsFolder = os.path.join(os.path.abspath(os.curdir), 'datasets')
    dataSets = ['adultContent', 'barzanMozafari', 'small', 'medium', 'big', 'gds', 'tagasauris']

    dataProcessor = ProcessData()
    currentFolder = os.path.abspath(os.curdir)
    os.path.join(currentFolder, 'datasets')
    algorithms = ['BMV', 'IMV', 'BDS', 'IDS']
    with open('dataPerf.csv', "wb") as csv_file:
        results_writer = csv.writer(csv_file, delimiter=',')
        results_writer.writerow(['DataSet', 'Algorithm', 'CreateClient', 'LoadAssigns', 'LoadGoldLabels', 'LoadEvaluationLabels', 'Compute', 'GetObjectPrediction', 'GetWorkerQualities'])
        for dataSet in dataSets:
            testFolder = os.path.join(dataSetsFolder, dataSet)
            print testFolder
            categoriesFile = os.path.join(testFolder, 'categories.txt')
            assignsFile = os.path.join(testFolder, 'assigns.txt')
            goldLabelsFile = os.path.join(testFolder, 'golds.txt')
            evaluationLabelsFile = os.path.join(testFolder, 'evaluation.txt')
            for algorithm in algorithms:
                print '--------'
                print algorithm
                client = TroiaClient(ADDRESS)

                categories = dataProcessor.time_data_load_call(lambda: dataProcessor.loadCategories(categoriesFile))
                goldLabels = dataProcessor.time_data_load_call(lambda: dataProcessor.loadGoldLabels(goldLabelsFile))
                evaluationLabels = dataProcessor.time_data_load_call(lambda: dataProcessor.loadEvaluationLabels(evaluationLabelsFile))
                assignedLabels = dataProcessor.time_data_load_call(lambda: dataProcessor.loadAssigns(assignsFile))

                print 'Creating client'
                createClient_t = dataProcessor.time_api_call(lambda: client.create(categories, algorithm=algorithm))

                print 'Post gold labels'
                loadGolds_t = dataProcessor.time_api_call(lambda: client.post_gold_data(goldLabels))

                print 'Post evaluation labels'
                loadEval_t = dataProcessor.time_api_call(lambda: client.post_evaluation_objects(evaluationLabels))

                print 'Post assigned labels'
                loadAssigns_t = dataProcessor.post_assigns(client, assignedLabels)

                print 'Computing'
                compute_t = dataProcessor.time_api_call(lambda: client.post_compute())

                print 'Getting object prediction'
                oPred_t = dataProcessor.time_api_call(lambda: client.get_objects_prediction("MinCost"))

                print 'Getting worker quality summary'
                wqEst_t =  dataProcessor.time_api_call(lambda: client.get_workers_quality_estimated_summary())

                results_writer.writerow([dataSet, algorithm, createClient_t, loadAssigns_t, loadGolds_t, loadEval_t, compute_t, oPred_t, wqEst_t])
