import csv
import math
from elasticsearch import helpers, Elasticsearch
from nltk import word_tokenize, sent_tokenize


class SearchEngine:
    es = ""

    def connect_to_elasticsearch(self):  # connect to elasticsearch server
        self.es = Elasticsearch(['http://localhost:9200/'], verify_certs=True, sniff_on_start=True,
                                sniff_on_connection_fail=True, sniffer_timeout=60)
        if not self.es.ping():
            raise ValueError("Connection failed")
        if self.es.ping():
            print("Connection succeeded")
            return True  # if able to success ping and confirm connection return true

    def file_reader(self, file):
            if self.connect_to_elasticsearch():
                try:
                    # open csv file to be read - file must be in same directory as python script
                    with open(file, "r") as file:
                        csvfile = csv.DictReader(file)
                        if self.es.indices.exists(index="testing"):  # check if index already exists
                            print("index already exists - deleting")
                            self.es.indices.delete(index="testing")  # deletes index if already exists in elasticsearch
                        dict_list = []  # list to store wiki contents
                        merge_list = []
                        for counter, row in enumerate(csvfile):
                            record = {}  # set to store
                            keys = row.keys()
                            for key in keys:
                                sentences = self.sentence_splitting(row[key])  # calls function to split values into sentences
                                tokens = self.handle_tokenize(sentences)  # calls function to tokenize sentences into words
                                normalize_words = self.normalize_words(tokens)  # calls function to normalize words
                                lowercase_words = self.lowercase_words(normalize_words)  # calls function to lowercase words
                                record[key] = row[key]  # adds key and values to dict
                                record[key + "_tokens"] = lowercase_words  # adds processed words to a new column in dict
                                merge_list = merge_list + lowercase_words  # adds all words into one list
                            if counter > 999:  # checks if processed entry so far doesnt exceend 1000
                                break
                            dict_list.append(record)
                        distinct_words = set(merge_list)  # unionise words so only distinct words remain
                        tf = self.computeTF(dict_list, distinct_words)  # calls function to calculate TF and returns results
                        idf = self.computeIDF(dict_list, counter)  # call function to calculate IDF and return results
                        helpers.bulk(self.es, dict_list, index="testing", doc_type="type")
                except IOError:  # prompts if the file cannot be opened
                    print("file cannot be opened")

    def sentence_splitting(self, param):
        return sent_tokenize(param)

    def handle_tokenize(self, sentences):
        array_of_tokens = []
        for tokens in sentences:
            array_of_tokens.append(word_tokenize(tokens))
        return array_of_tokens

    def normalize_words(self, tokens):
        normalized_words = []# array to store normalized words
        for elements in tokens:# loops through sub list element of tokenized words
            for word in elements: # loops through each word in the elements
                if word.isdigit() and len(word) > 2: # check if it is digits or length more than 2 to retain Release Year
                    normalized_words.append(word)# add to array
                    continue #continue loop to next word
                if word.isalpha():# # check if word is made up of chars
                    normalized_words.append(word)# add to array
        return normalized_words

    def lowercase_words(self, normalize_words):
        words_lowercase = []# array to store words
        for elements in normalize_words:# loops through elements
            words_lowercase.append(elements.lower())# lowercase words and add to the list
        return words_lowercase

    def computeTF(self, dict_list, distinct_words):
        new_list = []
        for row in dict_list: # loops through row of list
            copy_record = {}#dict to temp copy list
            keys = row.keys()# row keys
            for key in keys:# loops through keys
                copy_record[key] = row[key] # copy in tempt dict
                distinct_counts = dict.fromkeys(distinct_words, 0)# store discint words in dict and fill with 0's
                tfAlba = {}# temp dict
                if key[-7:] == "_tokens":# search keys that that end with _tokens
                    values = row[key]# store value
                    for word in values:# loop through all possible value - if matches with distinct words increment counter
                        distinct_counts[word] += 1
                    word_count = len(values)# length of values
                    if word_count == 0:# if there arent any empty - eg value of cast members are empty - break loop
                        break
                    for word, count in distinct_counts.items():# loop through key in dict and get count
                        tfAlba[word] = count / float(word_count)# divide number of times word appear by total words in current document
                    copy_record[key + "_tf"] = tfAlba# store results in a new column
            new_list.append(copy_record)# appends copy to list and return results
        return new_list

    def computeIDF(self, dict_list, counter):
        new_list = []
        merge_list = {}
        for row in dict_list: # loops through row of list
            copy_record = {}#dict to temp copy list
            keys = row.keys()# row keys
            for key in keys:# loops through keys
                copy_record[key] = row[key]# copy in tempt dict
                idfDict = dict.fromkeys(row[key], 0)# store row[key] values in dict and fill with 0's
                idfAlba = {}
                if key[-7:] == "_tokens":# search keys that that end with _tokens
                    values = row[key]
                    for word in values:
                        idfDict[word] += 1
                    word_count = len(values)
                    if word_count == 0:
                        break
                    for word in idfDict.keys():# loop through words in dict in order to merge them
                        if word not in merge_list.keys():# check if word not in new dict - add and increment
                            merge_list[word] = 1
                        else:# do not add and increment existing entry by 1
                            merge_list[word] += 1
                    for word, count in merge_list.items(): # loop through merged dict to get number of documents containing the word bing search for
                        idfAlba[word] = math.log(counter / float(count)) # divide the number of documents by documents that contain word
                    copy_record[key + "_idf"] = idfAlba# store results in a new column
            new_list.append(copy_record)# appends copy to list and return results
        return new_list
if __name__ == '__main__':
    SearchEngine().file_reader("wiki_movie_plots_deduped.csv")
