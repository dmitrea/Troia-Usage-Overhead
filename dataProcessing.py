from datetime import datetime
import time
import re
from client.gal import TroiaClient
from nominalJobsTests.testSettings import *
import csv
import os

class ProcessData():

    def loadAssigns(self, fileName):
        print 'Loading assigns file'
        assignedLabels = []
        for line in open(fileName,'r'):
            rowData = re.split('\t', line)
            workerName = rowData[0]
            objectName = rowData[1]
            assignedLabel = rowData[2].strip('\n')
            label = (workerName, objectName, assignedLabel)
            assignedLabels.append(label)
        return assignedLabels

    def loadCategories(self, fileName):
        print 'Loading categories file'
        categories=[]
        for category in open(fileName,'r'):
            categories.append(category.rstrip(os.linesep))
        return categories

    def loadGoldLabels(self, fileName):
        print 'Loading gold labels file'
        goldLabels = []
        try:
            for line in open(fileName,'r'):
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


if __name__ == '__main__':
    dataSetsFolder = os.path.join(os.path.abspath(os.curdir), 'datasets')
    dataSets = ['adultContent', 'barzanMozafari', 'small', 'medium', 'big', 'gds', 'tagasauris']

    dataProcessor = ProcessData()
    currentFolder = os.path.abspath(os.curdir)
    os.path.join(currentFolder, 'datasets')
    algorithms = ['BMV', 'IMV', 'BDS', 'IDS']
    with open('dataPerf.csv', "wb") as csv_file:
        results_writer = csv.writer(csv_file, delimiter=',')
        results_writer.writerow(['DataSet', 'Algorithm', 'ReadCategoriesFile', 'ReadAssignsFile', 'ReadGoldLabels', 'ReadEvaluationLabels', 'CreateClient', 'LoadAssigns', 'LoadGoldLabels', 'LoadEvaluationLabels', 'Compute', 'GetObjectPrediction', 'GetWorkerQualities'])
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
                initTime = datetime.now()
                categories = dataProcessor.loadCategories(categoriesFile)
                loadCategoryFile_t = datetime.now() - initTime
                print loadCategoryFile_t 

                initTime = datetime.now()
                assigns = dataProcessor.loadAssigns(assignsFile)
                loadAssignsFile_t = datetime.now() - initTime
                print loadAssignsFile_t

                initTime = datetime.now()
                goldLabels = dataProcessor.loadGoldLabels(goldLabelsFile)
                loadGoldsFile_t = datetime.now() - initTime
                print loadGoldsFile_t

                initTime = datetime.now()
                evaluationLabels = dataProcessor.loadEvaluationLabels(evaluationLabelsFile)
                loadEvalFile_t = datetime.now() - initTime
                print loadEvalFile_t

                initTime = datetime.now()
                response = client.create(categories, algorithm=algorithm)
                print response
                createClient_t = datetime.now() - initTime
                print createClient_t

                print 'Post assigned labels'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.post_assigned_labels(assigns))
                loadAssigns_t = datetime.now() - initTime
                print loadAssigns_t

                print 'Post gold labels'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.post_gold_data(goldLabels))
                loadGolds_t = datetime.now() - initTime
                print loadGolds_t

                print 'Post evaluation labels'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.post_evaluation_objects(evaluationLabels))
                loadEval_t = datetime.now() - initTime
                print loadEval_t

                print 'Computing'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.post_compute())
                compute_t = datetime.now() - initTime
                print compute_t

                print 'Getting object prediction'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.get_objects_prediction("MinCost"))
                oPred_t = datetime.now() - initTime
                print oPred_t

                print 'Getting worker quality summary'
                initTime = datetime.now()
                dataProcessor.check_status(client, client.get_workers_quality_estimated_summary())
                wqEst_t = datetime.now() - initTime
                print wqEst_t

                results_writer.writerow([dataSet, algorithm, loadCategoryFile_t, loadAssignsFile_t, loadGoldsFile_t, loadEvalFile_t, createClient_t, loadAssigns_t, loadGolds_t, loadEval_t, compute_t, oPred_t, wqEst_t])
