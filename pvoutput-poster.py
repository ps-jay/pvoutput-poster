import argparse
import httplib
import os
import sqlite3
import time
import threading

# XXX Todo: Convert to argparse
METER_DB = '/opt/energy/raven.sqlite'
SOLAR_DB = '/opt/energy/solar.sqlite'
TARIFF = {
    'peak': 0.3036,
    'offpeak': 0.1386,
    'peak_days': [1, 2, 3, 4, 5],
    'peak_times': [(7, 23)],
    'export': 0.08,
}

def gather_results(timestamp, meter_db, solar_db, include_prev_data=False):
    results = {}

    # Metering data
    db = sqlite3.connect(meter_db)
    cursor = db.cursor()

    cursor.execute('''
        SELECT * FROM metered
            WHERE timestamp <= %d
            ORDER BY timestamp DESC
            LIMIT 1
        ''' % timestamp)
    values = cursor.fetchall()
    try:
        if (timestamp - values[0][0]) < 300:
            results['Wh_in'] = values[0][1]
            results['Wh_out'] = values[0][2]
    except:
        pass

    if include_prev_data:
        ts_30m = timestamp - (60 * 30)
        cursor.execute('''
            SELECT * FROM metered
                WHERE timestamp <= %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % ts_30m)
        values = cursor.fetchall()
        try:
            if (ts_30m - values[0][0]) < 300:
                results['prev_Wh_in'] = values[0][1]
                results['prev_Wh_out'] = values[0][2]
        except:
            pass

    cursor.execute('''
        SELECT * FROM demand
            WHERE timestamp <= %d
            ORDER BY timestamp DESC
            LIMIT 1
        ''' % timestamp)
    values = cursor.fetchall()
    try:
        if (timestamp - values[0][0]) < 300:
            results['W_net'] = values[0][1]
    except:
        pass

    cursor.close()
    db.close()

    # Solar data
    db = sqlite3.connect(solar_db)
    cursor = db.cursor()

    cursor.execute('''
        SELECT * FROM system
            WHERE timestamp <= %d
            ORDER BY timestamp DESC
            LIMIT 1
        ''' % timestamp)
    values = cursor.fetchall()
    try:
        if (timestamp - values[0][0]) < 300:
            results['W_gen'] = values[0][1]
            results['Wh_gen'] = values[0][2]
    except:
        pass

    if include_prev_data:
        ts_30m = timestamp - (60 * 30)
        cursor.execute('''
            SELECT * FROM system
                WHERE timestamp <= %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % ts_30m)
        values = cursor.fetchall()
        try:
            if (ts_30m - values[0][0]) < 300:
                results['prev_Wh_gen'] = values[0][2]
        except:
            pass

    cursor.execute('''
        SELECT avg(Vin_V), avg(Tdsp_degC), avg(Tmos_degC) FROM panels
            WHERE (timestamp > %d) AND (timestamp <= %d)
        ''' % (
            (timestamp - 300),
            timestamp,
        ))
    values = cursor.fetchall()
    try:
        if values[0][0] is not None:
            results['Vin_avg'] = values[0][0]
        if values[0][1] is not None:
            results['Cdsp_avg'] = values[0][1]
        if values[0][2] is not None:
            results['Cmos_avg'] = values[0][2]
    except:
        pass

    cursor.close()
    db.close()

    return results

def calculate_pvoutput(timestamp, data, tariff=None):
    pvoutput = {}

    pvoutput['d'] = time.strftime("%Y%m%d", time.localtime(timestamp))
    pvoutput['t'] = time.strftime("%H:%M", time.localtime(timestamp))
    
    if 'Wh_gen' in data:
        pvoutput['v1'] = "%.0f" % data['Wh_gen']
    if 'W_gen' in data:
        pvoutput['v2'] = "%.0f" % data['W_gen']

    # Calculate consumption in Wh (param v3)
    # consumption = generation + import - export
    if (('Wh_in' in data) and
        ('Wh_out' in data) and
        ('Wh_gen' in data)):
        pvoutput['v3'] = "%.0f" % (
            data['Wh_gen'] + data['Wh_in'] - data['Wh_out']
        )
    
    if 'Vin_avg' in data:
        pvoutput['v6'] = "%.1f" % data['Vin_avg']
    if 'Cdsp_avg' in data:
        pvoutput['v7'] = "%.1f" % data['Cdsp_avg']
    if 'Cmos_avg' in data:
        pvoutput['v8'] = "%.1f" % data['Cmos_avg']

    pvoutput['c1'] = "1"

    # Remove once my metering actually works!! (and returns exported data)
    if (('v3' in pvoutput) and
        ('Wh_out' in data)):
        if data['Wh_out'] == 0:
            del(pvoutput['v3'])
        else:
            # Calculate consumption in W (param v4)
            # consumption = generation + net
            if (('W_net' in data) and
                ('W_gen' in data)):
                pvoutput['v4'] = "%.0f" % (data['W_gen'] + data['W_net'])

            if (('prev_Wh_out' in data) and
                ('prev_Wh_in' in data) and
                ('prev_Wh_gen' in data) and
                (tariff is not None)):
                if data['prev_Wh_out'] != 0:
                    # Calculate $ figures (at 0 and 30 minutes only)
                    imp = data['Wh_in'] - data['prev_Wh_in']
                    exp = data['Wh_out'] - data['prev_Wh_out']
                    gen = data['Wh_gen'] - data['prev_Wh_gen']
                    net = imp - exp
                    con = net + gen
                    day = int(time.strftime("%w", time.gmtime(timestamp)))
                    hour = int(time.strftime("%H", time.gmtime(timestamp)))
                    rate = tariff['offpeak']
                    if day in tariff['peak_days']:
                        for period in tariff['peak_times']:
                            if ((hour >= period[0]) and
                                (hour < period[1])):
                                rate = tariff['peak']
                                break
                    cost = (net / 1000.0) * rate
                    if net < 0:
                        cost = (net / 1000.0) * tariff['export']
                    pvoutput['v9'] = "%.2f" % (cost * 100)

    if (('v1' in pvoutput) or
        ('v2' in pvoutput) or
        ('v3' in pvoutput) or
        ('v4' in pvoutput)):
        return pvoutput
    
    return None

def bulk_main(t_start, t_end):
    for i in range(t_start, t_end):
        if (((int(time.strftime("%M", time.gmtime(i))) % 5) != 0) or
            ((int(time.strftime("%S", time.gmtime(i))) != 0))):
            continue

        t_search = i

        do_it(t_search)


def live_main():
    timer_exp = threading.Event()
    timer_exp.set()
    timer = None

    while True:
        # Wait for the timer to expire
        if not timer_exp.is_set():
            time.sleep(10)
            continue

        # Wait for a "5th" minute (0, 5, 10, .. 55)
        if (int(time.strftime("%M")) % 5) != 0:
            time.sleep(5)
            continue

        # Reset the timer
        timer_exp.clear()
        timer = threading.Timer(240, timer_exp.set)
        timer.start()

        # Calculate start of minute
        t_now = int(time.time())
        t_search = t_now - (t_now % 60)

        do_it(t_search)

def do_it(t_search):
        zero_thirty = (int(time.strftime("%M", time.gmtime(t_search))) % 30) == 0
        results = gather_results(t_search, METER_DB, SOLAR_DB, include_prev_data=zero_thirty)
        tariff = None
        if zero_thirty:
            tariff = TARIFF
        pvoutput = calculate_pvoutput(t_search, results, tariff)
        post(pvoutput)

def post(params):
    pvo_key = os.environ["API_KEY"]
    pvo_systemid = os.environ["SYSTEM_ID"]
    pvo_host= "pvoutput.org"
    pvo_statusuri= "/service/r2/addstatus.jsp"

    try:
        headers = {
            'X-Pvoutput-Apikey': pvo_key,
            'X-Pvoutput-SystemId': pvo_systemid,
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        out = "Data: "
        for param in sorted(iter(params)):
            out += "%s=%s; " % (param, params[param])
        print out
        
        conn = httplib.HTTPConnection(pvo_host)
        conn.request("POST", pvo_statusuri, urllib.parse.urlencode(params), headers)
        response = conn.getresponse()
        print("HTTP Status: ", response.status, "; Reason: ", response.reason, " - ", response.read())
        conn.close()
        return response.status == 200
    except Exception as e:
        print("Exception posting results\n", e)
        return False

if __name__ == "__main__":
    #bulk_main(1412088840, int(time.time()))
    live_main()
