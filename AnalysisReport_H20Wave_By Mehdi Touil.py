from h2o_wave import Q, main, app, ui, site, data
import pandas as pd
import gzip
try:
    from urllib.parse import unquote  # Python 3
except ImportError:
    from urllib import unquote       # Python 2

# Replace 'access_log.gz' with the correct path to your file
log_file_path = 'access_log.gz'


def parseApacheLogs(filename):
    fields = ['host', 'identity', 'user', 'time_part1', 'time_part2', 'cmd_path_proto',
              'http_code', 'response_bytes', 'referer', 'user_agent', 'unknown']
    data = pd.read_csv(filename, compression='gzip', sep=' ',
                       header=None, names=fields, na_values=['-'])

    # Panda's parser mistakenly splits the date into two columns, so we must concatenate them
    time = data.time_part1 + data.time_part2
    time_trimmed = time.map(lambda s: s.strip('[]').split(
        '-')[0])  # Drop the timezone for simplicity
    data['time'] = pd.to_datetime(time_trimmed, format='%d/%b/%Y:%H:%M:%S')

    # Split column `cmd_path_proto` into three columns, and decode the URL (ex: '%20' => ' ')
    data['command'], data['path'], data['protocol'] = zip(
        *data['cmd_path_proto'].str.split().tolist())
    data['path'] = data['path'].map(lambda s: unquote(s))

    # Drop the fixed columns and any empty ones
    data1 = data.drop(['time_part1', 'time_part2', 'cmd_path_proto'], axis=1)
    return data1.dropna(axis=1, how='all')


df = parseApacheLogs(log_file_path)

# Display the first few rows of the DataFrame
print(df.head())


# Example: Show a bar plot of HTTP response codes
page = site['/OutlierDetection']
tuples_array = []

# Group by 'http_code' and count occurrences
df_http_codes = df['http_code'].value_counts().reset_index()
df_http_codes.columns = ['http_code', 'count']
code_tuples = list(df_http_codes.itertuples(index=False, name=None))
code_tuples = [(str(code), count) for code, count in code_tuples]

# Extend the list of tuples
tuples_array.extend(code_tuples)
page.add('example', ui.wide_plot_card(
    box='1 1 5 5',
    title='Http Code Errors Plot Card',
    caption='''
  This plot illustrates the distribution of HTTP response codes denoting errors from the log data.
Each bar on the histogram represents a specific error code, while the height of the bar indicates the frequency or count of occurrences for that particular code.
This visualization enables us to see that our responses are most 401 Error code.
    ''',
    data=data('http_code count', 5, rows=code_tuples),
    plot=ui.plot(
        [ui.mark(type='interval', x='=http_code', y='=count', y_min=0)])
))

# Analysis of the host of errors
extracted_data = df[['host', 'path', 'http_code']]
# http_401_data = extracted_data[extracted_data['http_code'] == 401]
extracted_data['http_code'] = extracted_data['http_code'].astype(str)

ecolor_plot = ui.plot_card(
    box='6 1 4 7',
    title='Outlier Detection - analysis of host of errors',
    data=data('host	path http_code', 5,
              rows=extracted_data.values.tolist()),
    plot=ui.plot(
        marks=[ui.mark(type='interval', x='=host',
                       y='=path', color='=http_code')]
    )
)
page.add('ecolor', ecolor_plot)
# Add a card to the Wave app
card = page.add("my_card_id", ui.markdown_card(
    box="1 6 5 2",  # Define the position and size of the card
    title="Outlier Result _ By Mehdi Touil",
    content="From my analysis we discover that there 1037 request of 401 made from a specific IP address to the admin page. Also it can be noted from above there one request of 200  which indicates the login success after brute force attempt"
))
page.save()
