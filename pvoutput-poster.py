import argparse
import httplib
import os
import sqlite3
import time
import threading
import urllib


class PVOutputPoster():

    def __init__(self):
        # XXX Todo: Convert to argparse, or config file
        self.METER_DB = '/opt/energy/raven.sqlite'
        self.SOLAR_DB = '/opt/energy/solar.sqlite'
        self.PVO_DB = '/opt/energy/pvoutput.sqlite'
        self.WEATHER_JSON = '/var/opt/energy/weather.json'
        self.TARIFF = {
            'peak': 0.3036,
            'offpeak': 0.1386,
            'peak_days': [1, 2, 3, 4, 5],
            'peak_times': [(7, 23)],
            'export': 0.08,
        }

    def _get_meter_data(self, timestamp):
        results = {}

        # Metering data
        db = sqlite3.connect(self.METER_DB)
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

        # 5-min previous
        cursor.execute('''
            SELECT * FROM metered
                WHERE timestamp <= %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % (timestamp - 300)
        )
        values = cursor.fetchall()
        try:
            if (timestamp - 300 - values[0][0]) < 300:
                results['prev_Wh_in'] = values[0][1]
                results['prev_Wh_out'] = values[0][2]
        except:
            pass

        cursor.close()
        db.close()

        return results

    def _get_solar_data(self, timestamp):
        results = {}

        # Solar data
        db = sqlite3.connect(self.SOLAR_DB)
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
                results['Wh_gen'] = values[0][2]
        except:
            pass

        # 5-min previous
        cursor.execute('''
            SELECT * FROM system
                WHERE timestamp <= %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % (timestamp - 300)
        )
        values = cursor.fetchall()
        try:
            if (timestamp - 300 - values[0][0]) < 300:
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

    def _fake_Wh_out(self, timestamp):
        # Meter data
        db = sqlite3.connect(self.METER_DB)
        cursor = db.cursor()
        cursor.execute('''
            SELECT avg(watts) FROM demand
                WHERE (timestamp > ?) AND (timestamp <= ?) AND (watts < 0)
            ''', ((timestamp - 300), timestamp)
        )
        value = cursor.fetchall()
        cursor.close()
        db.close()

        if value == []:
            return 0
        if value[0][0] is None:
            return 0
        
        # 5min -> 1hr convert
        return value[0][0] / 12.0

    def _calculate_pvoutput(self, timestamp, data):
        pvoutput = {}

        pvoutput['d'] = time.strftime("%Y%m%d", time.localtime(timestamp))
        pvoutput['t'] = time.strftime("%H:%M", time.localtime(timestamp))
        
        if 'Wh_gen' in data:
            pvoutput['v1'] = "%.0f" % data['Wh_gen']

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
                pvoutput['v3'] = "%.0f" % (
                    data['Wh_gen'] + data['Wh_in'] - self._fake_Wh_out(timestamp)
                )
            elif data['Wh_out'] != 0:
                print "Wow!  Wh_out data exists!"
            elif (('prev_Wh_out' in data) and
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

    def post(self, params):
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
            conn.request("POST", pvo_statusuri, urllib.urlencode(params), headers)
            response = conn.getresponse()
            print("HTTP Status: ", response.status, "; Reason: ", response.reason, " - ", response.read())
            conn.close()
            return response.status == 200
        except Exception as e:
            print("Exception posting results\n", e)
            return False

    def _init_db(self, cursor):
        cursor.execute('''
            CREATE TABLE pvoutput (
                timestamp INTEGER PRIMARY KEY,
                v1 INTEGER,
                v2 INTEGER,
                v3 INTEGER,
                v4 INTEGER,
                v5 INTEGER,
                v6 INTEGER,
                v7 INTEGER,
                v8 INTEGER,
                v9 INTEGER,
                v10 INTEGER,
                v11 INTEGER,
                v12 INTEGER,
                need_upload INTEGER NOT NULL
            )
        ''')
        cursor.execute('''CREATE INDEX need_ul ON pvoutput (need_upload)''')

    def _get_last_entry(self, cursor):
        cursor.execute('''SELECT timestamp FROM pvoutput ORDER BY timestamp DESC LIMIT 1''')
        db_time = cursor.fetchall()
        if db_time == []:
            return 1411603200
        else:
            return db_time[0][0]

    def main(self):
        # XXX Consume temperature data
        # XXX API limit ?

        pvo_db = sqlite3.connect(self.PVO_DB)
        cursor = pvo_db.cursor()
        
        # argparse option?
        #self._init_db(cursor)
        
        t_start = int(self._get_last_entry(cursor) + 60)
        t_end = int(time.time() - 60)

        for t in range(t_start, t_end):
            if (((int(time.strftime("%M", time.gmtime(t))) % 5) != 0) or
                ((int(time.strftime("%S", time.gmtime(t))) != 0))):
                continue

            data = dict(
                self._get_meter_data(t).items() +
                self._get_solar_data(t).items()
            )
            pvoutput = self._calculate_pvoutput(t, data)

            if pvoutput is not None:
                # XXX POST, 200 = need_upload = 0; !200 = need_upload = 1
                cols = "timestamp, need_upload, "
                data = "%s, 1, " % t
                for key in pvoutput:
                    if ((key == 'c1') or
                        (key == 'd') or
                        (key == 't')
                    ):
                        continue
                    cols += "%s, " % key
                    data += '%s, ' % pvoutput[key]
                cursor.execute('''
                    INSERT INTO pvoutput (%s)
                        VALUES (%s)
                    ''' % (cols[:-2], data[:-2])
                )
        pvo_db.commit()

        # Search for missing v5's in the temp data range
        # Fill in missing v5's
        # reset need_ul if filled in v5
        cursor.close()
        pvo_db.close()

if __name__ == "__main__":
    pvo = PVOutputPoster()
    pvo.main()
