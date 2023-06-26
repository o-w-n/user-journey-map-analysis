import pandas as pd
from textblob import TextBlob
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 3000)
pd.set_option('display.max_rows', 2000)
df = pd.read_csv('fme_user_feedback.csv')

df['sentiment'] = df['text'].apply(lambda x: TextBlob(x).sentiment.polarity)
conditions = [
    df['sentiment'] > 0.5,
    df['sentiment'] > 0,
    df['sentiment'] == 0,
    df['sentiment'] < 0,
    df['sentiment'] < -0.5
]

choices = ['very good', 'good', 'normal', 'bad', 'hate']
df['sentiment_label'] = np.select(conditions, choices, default='normal')

counts = df['sentiment_label'].value_counts()
max_count = counts.max()

ratios = counts / max_count

updated_choices = [
    'very good' if ratios['very good'] > 0.1 else 'good',
    'good' if ratios['good'] > 0.2 else 'normal',
    'normal' if ratios['normal'] > 0.6 else 'bad',
    'bad' if ratios['bad'] > 0.1 else 'hate',
    'hate'
]

df['sentiment_label'] = np.select(conditions, updated_choices, default='normal')
print(df.to_csv('rate_of_feedback.csv'))
sentiment_counts = df['sentiment_label'].value_counts()
print("Sentiment Analysis Results:")
print(sentiment_counts)
