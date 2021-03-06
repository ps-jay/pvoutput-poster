import argparse
import calendar
import httplib
import json
import os
import sqlite3
import sys
import time
import threading
import urllib
import astral
import datetime


class PVOutputPoster():

    def __init__(self):
        # XXX Todo: Convert to argparse, or config file
        self.METER_DB = '/data/raven.sqlite'
        self.SOLAR_DB = '/data/solar.sqlite'
        self.PVO_DB = '/data/pvoutput.sqlite'
        self.WEATHER_JSON = '/data/weather.json'
        self.TARIFF = {
            'peak': 0.3080,
            'offpeak': 0.13915,
            'peak_days': [1, 2, 3, 4, 5],
            'peak_times': [(7, 23)],
            'export': 0.065,
        }

        self.INTERVAL = 600
        self.MODULO = (self.INTERVAL/60)
        self.WHCONVERT = (60/self.MODULO)

        # Always assume some load (in W)
        self.BASELOAD = 240

        self.PVO_KEY = os.environ["API_KEY"]
        self.PVO_SYSID = os.environ["SYSTEM_ID"]
        self.PVO_HOST = "pvoutput.org"
        self.PVO_ADDSTATUS = "/service/r2/addstatus.jsp"
        self.PVO_GETSTATUS = "/service/r2/getstatus.jsp"

        self.pvo_db = sqlite3.connect(self.PVO_DB)
        self.pvo_db.row_factory = sqlite3.Row
        self.cursor = self.pvo_db.cursor()

        self.location = astral.Location(
            info=(
                'Blackburn',
                'Victoria',
                -37.82,
                145.15,
                'Australia/Melbourne',
                50
            )
        )

    def _interpolate_value(self, t1, t2, v1, v2):
        delta_t = t2 - t1
        delta_v = v2 - v1
        return float(delta_v) / float(delta_t)

    def _median(self, list):
        ordered = sorted(list)
        samples = len(list)
        mid = (samples - 1) // 2

        if (samples % 2) == 1:
            return ordered[mid]
        else:
            return (ordered[mid] + ordered[mid + 1]) / 2.0

    def _lookup_meter_data(self, timestamp):
        results = {}

        # Metering data
        db = sqlite3.connect(self.METER_DB)
        cursor = db.cursor()

        cursor.execute('''
            SELECT * FROM metered
                WHERE timestamp < %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            results['Wh_in'] = values[0][1]
            results['Wh_out'] = values[0][2]
        except:
            return {}

        first_time = values[0][0]
        cursor.execute('''
            SELECT * FROM metered
                WHERE timestamp >= %d
                ORDER BY timestamp ASC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            second_time = values[0][0]

            inter_in = self._interpolate_value(
                first_time, second_time,
                results['Wh_in'], values[0][1],
            )
            inter_out = self._interpolate_value(
                first_time, second_time,
                results['Wh_out'], values[0][2],
            )
            ts_diff = timestamp - first_time
            results['Wh_in'] += inter_in * ts_diff
            results['Wh_out'] += inter_out * ts_diff
        except:
            return {}

        cursor.close()
        db.close()

        return results

    def _get_meter_data(self, timestamp):
        results = self._lookup_meter_data(timestamp)
        if results == {}:
            return results

        previous_results = self._lookup_meter_data(timestamp - self.INTERVAL)
        if 'Wh_in' in previous_results:
            results['prev_Wh_in'] = previous_results['Wh_in'] 
        if 'Wh_out' in previous_results:
            results['prev_Wh_out'] = previous_results['Wh_out'] 

        return results

    def _lookup_max_solar_data(self, timestamp):
        results = {}

        # Solar data
        db = sqlite3.connect(self.SOLAR_DB)
        cursor = db.cursor()

        cursor.execute('SELECT MAX(etot_Wh) FROM system WHERE timestamp < %d' % timestamp)
        values = cursor.fetchall()

        try:
            return values[0][0]
        except:
            return None

    def _lookup_solar_data(self, timestamp):
        results = {}

        # Solar data
        db = sqlite3.connect(self.SOLAR_DB)
        cursor = db.cursor()

        cursor.execute('''
            SELECT * FROM system
                WHERE timestamp < %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            results['Wh_gen'] = values[0][2]
        except:
            return {}

        # Find point #2
        first_time = values[0][0]
        cursor.execute('''
            SELECT * FROM system
                WHERE timestamp >= %d
                ORDER BY timestamp ASC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            second_time = values[0][0]

            inter_gen = self._interpolate_value(
                first_time, second_time,
                results['Wh_gen'], values[0][2],
            )
            ts_diff = timestamp - first_time
            results['Wh_gen'] += inter_gen * ts_diff
        except:
            return {}

        try:
            # Temperature & voltage data
            cursor.execute('''
                SELECT macrf, avg(Tdsp_degC), avg(Tmos_degC), avg(Vin_V) FROM panels
                    WHERE (timestamp >= %d) AND (timestamp <= %d)
                    GROUP BY macrf;
                ''' % (
                    timestamp - (self.INTERVAL / 2),
                    timestamp + (self.INTERVAL / 2),
                ))
            values = cursor.fetchall()
            t_dsp = []
            t_mos = []
            v_in = []
            for v in values:
                t_dsp.append(v[1])
                t_mos.append(v[2])
                v_in.append(v[3])
            results['Cdsp_avg'] = self._median(t_dsp)
            results['Cmos_avg'] = self._median(t_mos)
            results['Vin_avg'] = self._median(v_in)
        except:
            pass

        cursor.close()
        db.close()

        return results

    def _get_solar_data(self, timestamp):
        results = self._lookup_solar_data(timestamp)
        if results == {}:
            return results

        max = self._lookup_max_solar_data(timestamp - self.INTERVAL)
        previous_results = self._lookup_solar_data(timestamp - self.INTERVAL)
        if 'Wh_gen' in previous_results:
            results['prev_Wh_gen'] = previous_results['Wh_gen']
            if results['prev_Wh_gen'] < max:
                results['prev_Wh_gen'] = max

        return results

    def _fake_Wh_out(self, timestamp):
        # Meter data
        db = sqlite3.connect(self.METER_DB)
        cursor = db.cursor()
        cursor.execute('''
            SELECT avg(watts) FROM demand
                WHERE (timestamp > ?) AND (timestamp <= ?) AND (watts < 0)
            ''', ((timestamp - self.INTERVAL), timestamp)
        )
        value = cursor.fetchall()
        cursor.close()
        db.close()

        if value == []:
            return 0
        if value[0][0] is None:
            return 0
        
        # Wh convert convert
        return (value[0][0] / float(self.WHCONVERT)) * (-1)

    def _calculate_pvoutput(self, timestamp, data):
        pvoutput = {}

        # Have the prev v1 value at hand
        self.cursor.execute('''
            SELECT timestamp, v1 FROM pvoutput
                WHERE timestamp < %d
                ORDER BY timestamp DESC
                LIMIT 1
        ''' % (timestamp)
        )
        value = self.cursor.fetchall()
        if value == []:
            prev_v1 = 0
            prev_v1_ts = 0
        else:
            prev_v1 = value[0][1]
            prev_v1_ts = value[0][0]

        if 'Wh_gen' in data:
            # If the CDD solar basestation is restarted, it resets to 0Wh
            # until the panels talk to it again, protect against this...
            #
            # Alternatively, sometimes a panel is "lost" and the value drops
            # Protect against this too
            if int("%.0f" % data['Wh_gen']) < prev_v1:
                sys.stdout.write("%s" % time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp)))
                print "; Wh_gen: %s < prev: %s; using prev value" % (
                    int("%.0f" % data['Wh_gen']),
                    prev_v1,
                )
                data['Wh_gen'] = data['prev_Wh_gen']
        if 'prev_Wh_gen' in data:
            if prev_v1_ts < (timestamp - self.INTERVAL):
                # if there's no prev_v1, then this is probably inaccurate
                del(data['prev_Wh_gen'])

        if (('Wh_gen' in data) and
            ('prev_Wh_gen' in data)):
            # Don't "generate" after sunset (could happen if we have gaps in data)
            # Don't "generate" before sunrise (could happen if we have gaps in data)
            # (with 10 minutes grace...)
            if data['Wh_gen'] != data['prev_Wh_gen']:
                ts = time.localtime(timestamp)
                day = datetime.date(ts.tm_year, ts.tm_mon, ts.tm_mday)
                dt = datetime.datetime.fromtimestamp(
                    timestamp,
                    self.location.sunset(day).tzinfo
                )
                sr = self.location.sunrise(day)
                sr_adj = sr - datetime.timedelta(0, self.INTERVAL)
                ss = self.location.sunset(day)
                ss_adj = ss + datetime.timedelta(0, self.INTERVAL)
                if dt > ss_adj:
                    print "ERROR: generation after sunset setting to prev. value"
                    data['Wh_gen'] = prev_v1
                    data['prev_Wh_gen'] = prev_v1
                elif dt < sr_adj:
                    print "ERROR: generation before sunrise (%s)" % sr_adj
                    print "timestamp=%s; prev_Wh_gen=%s; Wh_gen=%s" % (
                        timestamp,
                        data['prev_Wh_gen'],
                        data['Wh_gen'],
                    )
                    sys.exit(52)

        if 'Wh_gen' in data:
            pvoutput['v1'] = "%.0f" % data['Wh_gen']

        # Remove once metering actually works
        if 'Wh_out' in data:
#            if int(data['Wh_out']) == 0:
            if (int(data['Wh_out']) == 0) or (timestamp >= 1416315600 and timestamp < 1416402000):
                data['Wh_out'] = self._fake_Wh_out(timestamp)
        if 'prev_Wh_out' in data:
#            if int(data['prev_Wh_out']) == 0:
            if int(data['prev_Wh_out']) == 0 or (timestamp - self.INTERVAL >= 1416315600 and timestamp - self.INTERVAL < 1416402000):
                self.cursor.execute('''
                    SELECT Wh_out FROM fake_export
                        WHERE timestamp < %d
                        ORDER BY timestamp DESC
                        LIMIT 1
                ''' % (timestamp)
                )
                value = self.cursor.fetchall()
                if value == []:
                    data['prev_Wh_out'] = 0
                else:
                    data['prev_Wh_out'] = value[0][0]
                data['Wh_out'] += data['prev_Wh_out']
                self.cursor.execute('''
                    INSERT INTO fake_export (timestamp, Wh_out)
                        VALUES (%d, %d)
                ''' % (
                    timestamp,
                    data['Wh_out']
                ))

        # Calculate consumption in Wh (param v3)
        # consumption = generation + import - export
        if (('Wh_in' in data) and
            ('Wh_out' in data) and
            ('Wh_gen' in data)):
            pvoutput['v3'] = "%.0f" % (
                data['Wh_gen'] + data['Wh_in'] - data['Wh_out']
            )

            # previous value
            self.cursor.execute('''
                SELECT v3 FROM pvoutput
                    WHERE timestamp < %d
                    ORDER BY timestamp DESC
                    LIMIT 1
            ''' % (timestamp)
            )
            value = self.cursor.fetchall()
            if ((value == []) or \
                (value[0][0] == None)):
                data['prev_Wh_cons'] = 0
            else:
                data['prev_Wh_cons'] = value[0][0]

            if 'v3' in pvoutput:
                # If current consumption is less than baseload, then adjust
                # (which is possible due to the meter counting in 
                # 100Wh increments, and the solar counting in 1Wh increments)
                bl_Wh = self.BASELOAD / float(self.WHCONVERT)
                if int(pvoutput['v3']) <= (data['prev_Wh_cons'] + bl_Wh):
                    pvoutput['v3'] = "%.0f" % (
                        data['prev_Wh_cons'] + bl_Wh
                    )

        if not (
            ('v1' in pvoutput) or
            ('v3' in pvoutput)
        ):
            return None
        
        air_temp = self._get_temp(timestamp)
        if air_temp is not None:
            pvoutput['v5'] = "%.1f" % air_temp

        if 'Vin_avg' in data:
            pvoutput['v6'] = "%.1f" % data['Vin_avg']
        if 'Cdsp_avg' in data:
            pvoutput['v7'] = "%.1f" % data['Cdsp_avg']
        if 'Cmos_avg' in data:
            pvoutput['v8'] = "%.1f" % data['Cmos_avg']

        if (('Wh_gen' in data) and
            ('prev_Wh_gen' in data) and
            ('prev_Wh_cons' in data) and
            ('v3' in pvoutput)):
                gen = data['Wh_gen'] - data['prev_Wh_gen']
                con = int(pvoutput['v3']) - data['prev_Wh_cons']
                net = con - gen
                day = int(time.strftime("%w", time.localtime(timestamp)))
                hour = int(time.strftime("%H", time.localtime(timestamp)))
                rate = self.TARIFF['offpeak']
                if day in self.TARIFF['peak_days']:
                    for period in self.TARIFF['peak_times']:
                        if ((hour >= period[0]) and
                            (hour < period[1])):
                            rate = self.TARIFF['peak']
                            break
                cost = (net / 1000.0) * rate
                if net < 0:
                    cost = (net / 1000.0) * self.TARIFF['export']
                pvoutput['v9'] = "%.2f" % (cost * 100)

        if self.verbose:
            sys.stdout.write("%s" % time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp)))
            if 'Wh_in' in data:
                sys.stdout.write("; import=%dWh (%dWh total)" % (
                    data['Wh_in'] - data['prev_Wh_in'],
                    data['Wh_in'],
                ))
            if 'Wh_out' in data:
                sys.stdout.write("; export=%dWh (%dWh total)" % (
                    data['Wh_out'] - data['prev_Wh_out'],
                    data['Wh_out'],
                ))
            if 'v3' in pvoutput:
                sys.stdout.write("; consume=%dWh (%dWh total)" % (
                    int(pvoutput['v3']) - data['prev_Wh_cons'],
                    int(pvoutput['v3']),
                ))
            if 'v1' in pvoutput:
                sys.stdout.write("; produce=%dWh (%dWh total)" % (
                    data['Wh_gen'] - data['prev_Wh_gen'],
                    int(pvoutput['v1']),
                ))
            if net is not None:
                sys.stdout.write("; net=%dWh" % net)
            if 'v9' in pvoutput:
                sys.stdout.write("; cost=%sc" % pvoutput['v9'])
            print ""

        if (('v1' in pvoutput) or
            ('v3' in pvoutput)):
            return pvoutput
        
        return None

    def _upload(self):
        try:
            headers = {
                'X-Pvoutput-Apikey': self.PVO_KEY,
                'X-Pvoutput-SystemId': self.PVO_SYSID,
                'X-Rate-Limit': '1',
            }
            conn = httplib.HTTPConnection(self.PVO_HOST)
            conn.request("GET", self.PVO_GETSTATUS, None, headers)
            response = conn.getresponse()
            remaining = response.getheader('x-rate-limit-remaining')
            if remaining is not None:
                remaining = int(remaining)
                if remaining <= 15:
                    print "ERROR: less than 15 API calls remaining"
                    return
            else:
                print "ERROR: didn't get a x-rate-limit-remaining result"
                return
        except Exception as e:
            print "ERROR: When quering API limit: %s" % str(e)
            return

        # Find stuff to upload
        self.cursor.execute('''
            SELECT * FROM pvoutput
                WHERE need_upload = 1
                ORDER BY timestamp ASC
                LIMIT %d
            ''' % (remaining - 15)
        )
        rows = self.cursor.fetchall()
        for row in rows:
            pvoutput = {}
            for col in row.keys():
                if ((col != 'timestamp') and
                    (col != 'need_upload')
                ):
                    if row[col] is not None:
                        pvoutput[col] = row[col]
            pvoutput['d'] = time.strftime("%Y%m%d", time.localtime(row['timestamp']))
            pvoutput['t'] = time.strftime("%H:%M", time.localtime(row['timestamp']))
            pvoutput['c1'] = "1"
            # print "-> %s %s" % (pvoutput['d'], pvoutput['t'])

            if self._post(pvoutput):
                self.cursor.execute('''
                    UPDATE pvoutput
                        SET need_upload = 0
                        WHERE timestamp = %d
                ''' % row['timestamp'])
                print "Posted %s %s" % (pvoutput['d'], pvoutput['t'])

    def _post(self, params):

        try:
            headers = {
                'X-Pvoutput-Apikey': self.PVO_KEY,
                'X-Pvoutput-SystemId': self.PVO_SYSID,
                "Accept": "*/*",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            conn = httplib.HTTPConnection(self.PVO_HOST)
            conn.request("POST", self.PVO_ADDSTATUS, urllib.urlencode(params), headers)
            response = conn.getresponse()
            conn.close()
            if response.status == 200:
                return True
            else:
                print "HTTP POST Failed: code %d; reason %s" % (
                    response.status,
                    response.reason,
                )
                return False
        except Exception as e:
            print "Exception with HTTP POST: %s" % str(e)
            return False

    def _init_db(self):
        self.cursor.execute('''
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
        self.cursor.execute('''CREATE INDEX need_ul ON pvoutput (need_upload)''')
        self.cursor.execute('''CREATE INDEX has_temp ON pvoutput (v5)''')
        self.cursor.execute('''
            CREATE TABLE temperature (
                timestamp INTEGER PRIMARY KEY,
                degC REAL NOT NULL
            )
        ''')

    def _get_last_entry(self):
        self.cursor.execute('''SELECT timestamp FROM pvoutput ORDER BY timestamp DESC LIMIT 1''')
        db_time = self.cursor.fetchall()
        if db_time == []:
            return 1411603200
        else:
            return db_time[0][0]

    def _get_temperature_data(self):
        with open(self.WEATHER_JSON, 'rb') as fh:
            data = json.load(fh)['observations']['data']
        temps = {}
        for obs in data:
            try:
                epoch = calendar.timegm(
                    time.strptime(obs['aifstime_utc'], "%Y%m%d%H%M%S")
                )
                temps["%.0f" % epoch] = obs['air_temp']
            except:
                print "ERROR: Issue with BOM data"

        return temps

    def _update_temperature_db(self, temps):
        for temp in temps:
            if temps[temp] is None:
                continue
            self.cursor.execute('''
                INSERT OR REPLACE INTO temperature VALUES (
                    ?, ?
                )
            ''', (temp, temps[temp],)
            )

    def _get_temp(self, timestamp):
        self.cursor.execute('''
            SELECT * FROM temperature
                WHERE timestamp <= %d
                ORDER BY timestamp DESC
                LIMIT 1
            ''' % timestamp)
        try:
            first_value = self.cursor.fetchall()[0]
            if (timestamp - first_value[0]) > 3600:
                return None
            elif (timestamp - first_value[0]) == 0:
                return first_value[1]
        except:
            return None

        self.cursor.execute('''
            SELECT * FROM temperature
                WHERE timestamp > %d
                ORDER BY timestamp ASC
                LIMIT 1
            ''' % timestamp)
        try:
            second_value = self.cursor.fetchall()[0]
            if (second_value[0] - timestamp) > 3600:
                return None
            elif (second_value[0] - timestamp) == 0:
                return second_value[1]

            inter_t = self._interpolate_value(
                first_value[0], second_value[0],
                first_value[1], second_value[1],
            )
            ts_diff = timestamp - first_value[0]
            return first_value[1] + (inter_t * ts_diff)
        except:
            return None

    def _fill_in_temperatures(self, t_start, t_end):
        self.cursor.execute('''
            SELECT timestamp FROM pvoutput
                WHERE v5 is NULL''')
        nov5 = self.cursor.fetchall()
        for row in nov5:
            temp = self._get_temp(row[0])
            if temp is None:
                continue
            self.cursor.execute('''
                UPDATE pvoutput
                    SET v5 = %.1f, need_upload = 1
                    WHERE timestamp = %d
            ''' % (temp, row[0]))

    def main(self):

        # make an argparse option?
        # self._init_db()
        # exit(1)

        # another arg
        self.verbose = True

        temps = self._get_temperature_data()
        if temps != {}:
            self._update_temperature_db(temps)

        t_start = int(self._get_last_entry() + 60)
        t_end = int(time.time() - (self.INTERVAL))

        for t in range(t_start, t_end):
            if (((int(time.strftime("%M", time.gmtime(t))) % self.MODULO) != 0) or
                ((int(time.strftime("%S", time.gmtime(t))) != 0))):
                continue
            # print time.strftime("%Y%m%d %H:%M", time.localtime(t))

            meter = self._get_meter_data(t).items()
            solar = self._get_solar_data(t).items()
            # At this point, meter & solar are lists with tuples

            if (((solar == []) or (meter == [])) and \
                (t > (time.time() - 24 * 60 * 60))):
                # if solar or meter has no data, and
                # 't' is not more than 24-hours ago, then
                # wait for data (i.e. up to 24 hours for data to appear)
                sys.stdout.write("%s" % time.strftime("%Y-%m-%d %H:%M", time.localtime(t)))
                if solar == []:
                    sys.stdout.write("; (no solar data)")
                if meter == []:
                    sys.stdout.write("; (no meter data)")
                print "; waiting..."
                self.pvo_db.commit()
                continue

            data = dict(
                meter +
                solar
            )

            pvoutput = self._calculate_pvoutput(t, data)
            if pvoutput is None:
                sys.stdout.write("%s" % time.strftime("%Y-%m-%d %H:%M", time.localtime(t)))
                print "; (pvoutput is None)"
                self.pvo_db.commit()
                self.cursor.close()
                self.pvo_db.close()
                sys.exit(51)
            else:
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
                self.cursor.execute(
                '''
                    INSERT INTO pvoutput (%s)
                        VALUES (%s)
                    ''' % (cols[:-2], data[:-2])
                )
                self.pvo_db.commit()

        self._fill_in_temperatures((t_end - (4 * 24 * 60 * 60)), t_end)
        self.pvo_db.commit()
       
        self._upload()
        
        self.pvo_db.commit()
        self.cursor.close()
        self.pvo_db.close()

if __name__ == "__main__":
    pvo = PVOutputPoster()
    pvo.main()
