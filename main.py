__author__ = 'yong8'
# -*- coding: utf-8 -*-

import xlrd
import xlwt
import csv
import codecs
import datetime
import sqlite3
import sys
import re
import requests
import json
import sqlite3
import simplejson
from konlpy.tag import Twitter

reload(sys)
sys.setdefaultencoding('utf-8')

from konlpy.tag import Kkma
from konlpy.utils import pprint

#workbooks = []
#worksheets = []

def xlsxToSql():
    conn = sqlite3.connect("../tweets_mers.db")
    conn.text_factory = str
    cursor = conn.cursor()

    for index in range(0,22):
        workbook1 = xlrd.open_workbook("../tweets/solr%d.xlsx" % index, "r", encoding_override="utf-8")
        worksheet = workbook1.sheet_by_index(0)
        nrows = worksheet.nrows

        for row_num in range(nrows):
            row_value = worksheet.row_values(row_num)

            if row_num != 0:
                print row_value[0]
                cursor.execute('INSERT INTO tweets (docid, date, text) values(?,?,?);', (row_value[0], row_value[3], row_value[7]))

        conn.commit()
    conn.close()
"""
    for index in range(0):
        workbook1 = xlrd.open_workbook("./tweets/solr%d.xlsx" % index, "r", encoding_override="utf-8")
        file1 = codecs.open("korean.txt", "w", "utf-8")
        worksheet = workbook1.sheet_by_index(0)
        nrows = worksheet.nrows

        for row_num in range(nrows):
            row_value = worksheet.row_values(row_num)
            conn.execute('INSERT INTO tweets ('docid', 'date', 'text') values(?,?,?);', ())
"""



def countNumOfTweets():
    nrows = []
    tweets = []
    tweets_by_time = {}
    for index in range(22):
        workbook1 = xlrd.open_workbook("./tweets/solr%d.xlsx" % index, "r", encoding_override="utf-8")
        file1 = codecs.open("korean.txt", "w", "utf-8")
        worksheet = workbook1.sheet_by_index(0)
        nrows = worksheet.nrows


        for row_num in range(nrows):
            #print worksheet.row_values(row_num)
            row_value = worksheet.row_values(row_num)
            #tweet.append(row_value[4])

            if not row_num == 0:
                date = datetime.datetime.strptime(row_value[3], '%Y-%m-%d %H:%M:%S')
                tt = date.timetuple()
                # 0:year, 1:month, 2:day, 3:hour, 4:minute, 5:second
                month = tt[1]
                day = tt[2]
                hour = tt[3]

                if day <= 9:
                    day = "0" + str(day)
                if hour <= 9:
                    hour = "0" + str(hour)
                date = int(str(month) + str(day) + str(hour))

                #print date

                if date in tweets_by_time.keys():
                    tweets_by_time[date].append(row_value[7])
                else:
                    tweets_by_time[date] = []
                    tweets_by_time[date].append(row_value[7])

    with open('numberOfTweets.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for key, tweet in sorted(tweets_by_time.items()):
            print key, len(tweet)
            writer.writerow([key, len(tweet)])

def tokenization(text):
    text = str(unicode(text).encode('utf-8'))
    #print text
    token_list = text.decode("utf-8").split(' ')
    #print repr(token_list).decode('unicode-escape')
    return token_list

def remove(text):
    user_tag = '@' + str(set([i[1:] for i in text.split() if i.startswith("@")]))
    #text = text.remove(user_tag)
    #print text

    # Remove at tags
    regAt = re.compile('@([a-zA-Z0-9]*[*_/&%#@$]*)*[a-zA-Z0-9]*')
    text = re.sub(regAt, '', text)

    # Remove hashtags
    regHash = re.compile('#([a-zA-Z0-9]*[*_/&%#@$]*)*[a-zA-Z0-9]*')
    text = re.sub(regHash, '', text)

    # Remove url
    regUrl = re.compile('http([a-zA-Z0-9]*[*_/&%#@$.:]*)*[a-zA-Z0-9]*')
    text = re.sub(regUrl, '', text)

    text = re.sub('[!@#$%^&*():|.]', '', text)
    text = text.strip('ㅠ')

    return text

#def remove_stopwords()

# 품사처리. 처리함과 동시에 기본형까지 복원해준다.
# input: text(text after removal), output: tuple list (TERM, POS)
def pos_tagging(text):
    available_terms_list = []

    twitter = Twitter()
    pos_list = twitter.pos(text, norm=True, stem=True)

    for item in pos_list:
        if (item[1] == 'Verb') | (item[1] == 'Adjective'):
            available_terms_list.append(item)

    return available_terms_list

def pos_tagging_noun(text):
    noun_terms_list = []

    twitter = Twitter()
    pos_list = twitter.pos(text, norm=True, stem=True)

    for item in pos_list:
        if (item[1] == 'Noun'):
            noun_terms_list.append(item)

    return noun_terms_list

# 기본형 복 - pos_tagging에서 한번에 다 처리함. 고로 안씀
def restore_basic_form(word):
    basic_form = requests.get('http://api.openhangul.com/basic?q=%s' % word)
    basic_word = basic_form.json()['basic_word']
    return basic_word

def sentiment(basic_form):
    #try:
        sentiment_result = requests.get('http://api.openhangul.com/dic?api_key=mcom757320151023235717&q=%s' % basic_form)
        sentiment_result_json = sentiment_result.json()
        print repr(sentiment_result_json).decode('unicode-escape')
        return sentiment_result_json['sentiment']
    #except KeyError:
    #    print 'KeyError'

# 단어의 종류만 추려낸다 (단어의 종류, 등장횟수)
# input: 토큰들 from available_words, noun_words -> output: 엑셀파일 - 단어의 종류와 등장횟수를 리스트업
def extract_available_words():
    conn = sqlite3.connect('../tweets_mers.db')
    cursor5 = conn.cursor()
    rows = cursor5.execute("select noun_words from tweets")

    ### Extract words
    # Open csv file to write words kind
    file_training_result = open('../noun_words_kind.csv', "w")
    writer = csv.writer(file_training_result, delimiter=',')

    available_words_kind_dic = {}
    for tuple in rows:  # lists of Available words from database
        available_words = tuple[0]
        #print available_words
        available_words_list = available_words.split(',')
        #print available_words_list
        for available_word in available_words_list:  # word instances from database
            #print available_word
            # available_words_kind_list = ([available_words_kind], [# of occurrence])
            available_words_kind_list = available_words_kind_dic.keys() # available_words_kind_list = a set of keys of available_words_kind_dic
            if available_word not in available_words_kind_list:
                #print available_word
                available_words_kind_dic[available_word] = 1
            else:
                available_words_kind_dic[available_word] += 1  # Increase the count

    # Write words kind to csv file
    for key, value in available_words_kind_dic.items():
        print key, value
        writer.writerow([key, value])

    conn.commit()
    conn.close()


def main():
    # At the initial stage, use once
    #xlsxToSql()

    conn = sqlite3.connect('../tweets_mers.db')
    cursor = conn.cursor()

    rows = cursor.execute('select * from tweets')

    ### Iterating over texts, do pre-processing
    ### Insert text_after_removal into database
    text_list_after_removal = []
    cursor1 = conn.cursor()
    for row in rows:
        text = row[2]
        text_after_removal = remove(text)
        # USE ONLY ONCE for inserting: Insert texts after removal into database
        cursor1.execute("update tweets SET text_after_removal=? where docid=?", (text_after_removal, row[0]))
        conn.commit()


    conn.close()
'''
    ### Pull out 'available_words', analyze sentiment scores and
    unavailable_words_list = ['하다', '있다', '되다', '돼다', '이다', '뭐라다', '되어다', '대다', '나다', '어떻다', '허다']
    rows = cursor.execute('select * from tweets')
    cursor3 = conn.cursor()
    for key, row in enumerate(rows):
        pos_count = 0
        neg_count = 0
        if key <= 1000:
            if key >= 155:
                try:
                    print key
                    available_words_list = row[4].split(',')
                    for available_words in available_words_list:
                        if available_words not in unavailable_words_list:
                            if sentiment(available_words) == '긍정':
                                print repr(available_words).decode('unicode-escape')
                                pos_count += 1
                            elif sentiment(available_words) == '부정':
                                print repr(available_words).decode('unicode-escape')
                                neg_count += 1
                    cursor3.execute('update tweets SET num_of_pos_words=?, num_of_neg_words=? where docid=?', (pos_count, neg_count, row[0]))
                except ValueError:
                    print 'Decoding JSON has failed'
        else:
            break
        conn.commit()


'''

'''


    ### Pull out 'text_after_removal', do pos-tagging, get basic forms, save them to 'available_words'
    rows = cursor.execute('select * from tweets')
    cursor2 = conn.cursor()
    unavailable_words_list = ['하다', '있다', '되다', '돼다', '이다', '뭐라다', '되어다', '대다', '나다', '어떻다', '허다']
    for row in rows:
        print row[3]
        available_basic_terms_tuples = pos_tagging(row[3])  # availablte_terms_list is a list of tuples ('좋다', Verb). 기본형까지 다 복원된 상태
        available_basic_term_list = []
        # Gather only text('좋다') from tuple('좋다', Verb)
        for available_basic_term in available_basic_terms_tuples: # available_term = ('좋', Verb)
            if available_basic_term[0] not in unavailable_words_list:
                print available_basic_term[0]
                available_basic_term_list.append(available_basic_term[0])

        available_basic_terms_into_one_string = ','.join([basic_term for basic_term in available_basic_term_list])
        #print repr(available_terms_list).decode('unicode-escape')
        cursor2.execute("update tweets SET available_words=? where docid=?", (available_basic_terms_into_one_string, row[0]))

    #extract_available_words()

    ### EXTRACT ONLY NOUNS: Pull out 'text_after_removal', do pos-tagging, get basic forms, save them to 'available_words'
    rows = cursor.execute('select * from tweets')
    cursor2 = conn.cursor()
    for row in rows:
        available_basic_terms_tuples = pos_tagging_noun(row[3])  # availablte_terms_list is a list of tuples ('좋', Verb)
        available_basic_term_list = []
        # Gather only text('좋다') from tuple('좋다', Verb)
        for available_basic_term in available_basic_terms_tuples: # available_term = ('좋', Verb)
            print available_basic_term[0]
            available_basic_term_list.append(available_basic_term[0])

        available_basic_terms_into_one_string = ','.join([basic_term for basic_term in available_basic_term_list])
        print available_basic_terms_into_one_string
        #print repr(available_terms_list).decode('unicode-escape')
        cursor2.execute("update tweets SET noun_words=? where docid=?", (available_basic_terms_into_one_string, row[0]))
        conn.commit()
'''







    #text = restore_basic_form()
    #print unicode(text[0])

    #sentiment_word = "좋다"
    #sentiment(sentiment_word)


"""
    file1 = open("output.txt", "w")

    #kkma = Kkma()
    #pprint(kkma.pos(u'끙.....메르스가 그렇게 무섭다던데......오빠@actorjonghyuk 걱정때문에 밤새 잠을 못 이룰지경..........세상의 모든 나쁜것들은 오빠를 비껴가라ㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠ무서워서외출도못하게썽 ㅠㅠㅠㅠㅠ'))
    ex_text = '끙.....메르스가 그렇게 무섭다던데......오빠@actorjonghyuk 걱정때문에 밤새 잠을 못 이룰지경..........세상의 모든 나쁜것들은 오빠를 비껴가라ㅠㅠㅠㅠㅠㅠㅠㅠㅠㅠ무서워서외출도못하게썽 ㅠㅠㅠㅠㅠ'
    #print ex_text
    ex_text = str(unicode(ex_text, 'utf-8').encode('utf-8'))
    #print ex_text
    token_list = tokenization(ex_text)
    remove()
    available_terms_list = pos_tagging(ex_text)

    verb_list = []
    adj_list = []
    for item in available_terms_list:
        # Extract adjectives
        if item[1] == 'VA':
            adj_list = item[0]
        # Extract verbs
        elif item[1] == 'VV':
            verb_list = item[0]
"""


# ' '.join([word for word in line.split() if word != excludedWord]))
    #print [ str(unicode(item, 'utf-8').encode('utf-8')) for item in token_list ]

main()











