import datetime
import sys

# process logs into a list containing lists.
# each list is transformed from a log.
def get_logs(filename):
    logs = []
    print "logs processing..."
    with open(filename, 'r') as file:
        for line in file:
            record = []       
            addr, info = line.split(" - - ")
            # host address appended.
            record.append(addr)
            tokens = info.split("\"")
            time = (tokens[0])[1:-2]
            # time string appended.
            record.append(time)
            method_src = tokens[1].split(' ', 1)
            # method appended.
            record.append(method_src[0])
            # There are a few logs which have a different format.
            # So it is essential to judge the length first.
            if len(method_src) > 1:
                # source appended.
                record.append(method_src[1].split()[0])
            status, duration = tokens[-1].split()
            # status, duration appended.
            record.append(status)
            record.append(duration)
            # log appended.
            logs.append(record)
    print "logs processing finished."
    return logs

# write the result to a txt file.
def write_to_txt(li, filename):
    with open(filename, 'w') as file:
        for ele in li:
            file.write(ele + '\n')

# reverse the log to the original string format.
# used by feature 4.
def rever_transformation(li):
    res = []
    for ele in li:
        date_str = ele[1].strftime("%d/%b/%Y:%H:%M:%S") + ' -0400'
        request = ele[2] + ' ' + ele[3] + ' ' + 'HTTP/1.0'
        line = ele[0] + ' - - [' + date_str + "] \"" + request + "\" " + ele[4] + ' ' + ele[5]
        res.append(line)
    return res

# bucket sort function for feature 1 & 2.
def bucket_sort(bucket_num, li_li):
    top_10 = []
    cnt = 10
    for i in range(bucket_num - 1, -1, -1):
        if li_li[i]:
            for ele in li_li[i]:
                top_10.append([ele, i + 1])
                cnt -= 1
                if cnt == 0:
                    break
        if cnt == 0:
            break
    return top_10

# feature 1 extraction function.
def feature_1(logs):
    print "Feature 1 top 10 active host extracting..."
    # this block is to get the dictionary of host address.
    # key is address, value is the times of accessing the website.
    addr_book = {}
    bucket_num = len(logs)
    for log in logs:
        if log[0] in addr_book:
            addr_book[log[0]] += 1
        else:
            addr_book[log[0]] = 1

    # this block is to put each k-v pair to buckets.
    li_li = [[] for i in range(bucket_num)]
    for key, val in addr_book.iteritems():
        li_li[val - 1].append(key)

    # bucket sort.
    top_10_addr = bucket_sort(bucket_num, li_li)
    # prepare result to be written.
    res = []
    for ele in top_10_addr:
        res.append(ele[0] + ',' + str(ele[1]))
    print "Feature 1 top 10 active host extracted."
    return res

# feature 2 extraction function.
def feature_2(logs):
    print "Feature 2 top 10 sources extracting..."
    # this block is to get total bytes transfered for each source.
    # key is the source, value is a tuple of total bytes & accessing times.
    # Also, record the maximum bandwidth.
    src_book = {}
    maximum_band = 0
    for log in logs:
        # ensure the format of log is legal.
        if len(log) > 5 and log[-1] != '-':
            if log[3] in src_book:
                src_book[log[3]][0] += 1
                src_book[log[3]][1] += int(log[-1])  
            else:
                src_book[log[3]] = [1, int(log[-1])]
            cur = src_book[log[3]][1] / src_book[log[3]][0]
            maximum_band = max(maximum_band, cur)

    # the maximum bandwidth is the number of buckets.
    bucket_num = maximum_band
    # create buckets and assign k-v to buckets.
    li_li = [[] for i in range(bucket_num)]
    for key, val in src_book.iteritems():
        index = val[1] / val[0] - 1
        if index >= 0:
            li_li[index].append(key)

    # bucket sort.
    top_10_src = bucket_sort(bucket_num, li_li)
    # prepare result to be written.
    res = []
    for ele in top_10_src:
        res.append(ele[0])
    print "Feature 2 top 10 sources extracted."
    return res

# feature 3 extraction function.
def feature_3(logs):
    print "Feature 3 top 10 busiest time periods extracting..."
    # time_book is the dictionary with time-number pair.
    # key is starting time of a 60-min window.
    # value is the number of accessing attempt in that window.
    time_book = {}
    # put first entry into the book to avoid empty.
    start = datetime.datetime.strptime(logs[0][1].split(' -')[0], '%d/%b/%Y:%H:%M:%S')
    time_book[start] = 1
    length = len(logs)
    bucket_num = 0
    for i in range(length):
        time = logs[i][1].split(' -')[0]
        # create datetime object for each log.
        cur_time = datetime.datetime.strptime(time, '%d/%b/%Y:%H:%M:%S')
        # also update the time in the list of logs.
        logs[i][1] = cur_time
        # within 60 mins, number of current start time increase by 1.
        # else, update the start time.
        if (cur_time - start).total_seconds() <= 3600:
            time_book[start] += 1
        else:
            # update the bucket_num here.
            bucket_num = max(bucket_num, time_book[start])
            # get the first entry after the current start time.
            # and it will be the new start time.
            j = i - time_book[start] + 1
            start_next = logs[j][1]
            # if the new start time is not within the 60-min window of current log's time,
            # go the next entry of it until we find a legal new start time.
            while (cur_time - start_next).total_seconds() > 3600:
                j += 1
                start_next = logs[j][1]
            # initialize the value of the this new start time key.
            time_book[start_next] = i - j + 1
            start = start_next
    # don't forget to compare the last entry.
    bucket_num = max(bucket_num, time_book[start])

    # create buckets and put k-v pairs.
    li_li = [[] for i in range(bucket_num)]
    for key, val in time_book.iteritems():
        li_li[val - 1].append(key)

    # bucket sort.
    top_10_time = []
    cnt = 10
    for i in range(bucket_num - 1, -1, -1):
        # if bucket is not null.
        if li_li[i]:
            # iterate the bucket.
            for ele in li_li[i]:
                # if top_10_time is not null.
                if top_10_time:
                    tag = 1
                    # iterate them to see whether time overlapping exists.
                    for existed in top_10_time:
                        if abs((existed[0] - ele).total_seconds()) <= 3600:
                            tag = 0
                            break
                    # if overlapping not existed, add it into the result list.
                    if tag == 1:
                        top_10_time.append((ele, i + 1))
                        cnt -= 1
                else:
                    top_10_time.append((ele, i + 1))
                    cnt -= 1
                if cnt == 0:
                    break
        if cnt == 0:
            break

    # prepare result to be written.
    res = []
    for ele in top_10_time:
        date_str = ele[0].strftime("%d/%b/%Y:%H:%M:%S") + ' -0400'
        res.append(date_str + ',' + str(ele[1]))
    print "Feature 3 top 10 busiest time periods extracted."
    return res

# feature 4 extraction function.
def feature_4(logs):
    print "Feature 4 blocked attempts extracting..."
    # a book to store the 20s windows.
    start_book = {}
    # a lookup book for blocked address.
    block_book = {}
    res = []
    for log in logs:
        # filter out illegal logs.
        if len(log) < 6:
            continue
        # for all kinds of access, look up in the block book.
        if log[0] in block_book:
            # if within 300s window, add this log to result list.
            # and then continue to the next log.
            if (log[1] - block_book[log[0]]).total_seconds() <= 300:
                res.append(log)
                continue
            else:
                # else delete the block entry.
                # unlock the block address.
                del block_book[log[0]]
        # if not login attempt, need not to do further steps.
        if log[3] != '/login':
            continue
        # if a fail login.
        if log[4] != '200':
            if log[0] in start_book:
                # if within 20s window, failed login attempt counted.
                if (log[1] - start_book[log[0]][0]).total_seconds() <= 20:
                    start_book[log[0]].append(log[1])
                    # if 3 failed login attempt accumulated in 20s window,
                    # add the host address into the blocked book.
                    if len(start_book[log[0]]) == 3:
                        block_book[log[0]] = log[1]
                        del start_book[log[0]]
                # update the start point of 20s window.
                else:
                    start_book[log[0]] = [log[1]]
            else:
                start_book[log[0]] = [log[1]]
        # else delete the entry in start book.
        elif log[0] in start_book:
            del start_book[log[0]]
    print "Feature 4 blocked attempts extracted."
    return res

def main(args):
    # logs processed.
	logs = get_logs(args[1])

    # feature 1 constructed.
	feat_1 = feature_1(logs)
	write_to_txt(feat_1, args[2])

    # feature 2 constructed.
	feat_2 = feature_2(logs)
	write_to_txt(feat_2, args[4])

    # feature 3 constructed.
	feat_3 = feature_3(logs)
	write_to_txt(feat_3, args[3])

    # feature 4 constructed.
	feat_4 = feature_4(logs)
	feat_4 = rever_transformation(feat_4)
	write_to_txt(feat_4, args[5])

if __name__ == '__main__':
	main(sys.argv)