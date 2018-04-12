from p1_word_count import wordCount
from p1_counting_words import countingWords
from time import time

file_name=str(raw_input("Input a filename : "))

start_time = time()
dictionary=wordCount(file_name)
time_countWord=time()-start_time

start_time = time()
num_words=countingWords(file_name)
time_count=time()-start_time

print dictionary
print "Num total paraules: " + str(num_words)
print( "Temps transcorregut per Count Word: "+str(time_countWord)+ " segons")
print( "Temps transcorregut per Word Count: "+str(time_count)+ " segons")
