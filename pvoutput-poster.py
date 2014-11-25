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

        self.INTERVAL = 600
        self.MODULO = (self.INTERVAL/60)
        self.WHCONVERT = (60/self.MODULO)

        # Always assume some load (in W)
        self.BASELOAD = 200

        self.PVO_KEY = os.environ["API_KEY"]
        self.PVO_SYSID = os.environ["SYSTEM_ID"]
        self.PVO_HOST = "pvoutput.org"
        self.PVO_ADDSTATUS = "/service/r2/addstatus.jsp"
        self.PVO_GETSTATUS = "/service/r2/getstatus.jsp"

        self.pvo_db = sqlite3.connect(self.PVO_DB)
        self.pvo_db.row_factory = sqlite3.Row
        self.cursor = self.pvo_db.cursor()

    def _interpolate_value(self, t1, t2, v1, v2):
        delta_t = t2 - t1
        delta_v = v2 - v1
        return float(delta_v) / float(delta_t)

    def _lookup_meter_data(self, timestamp):
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
        if values == []:
            return {}

        interpolation_needed = True
        try:
            results['Wh_in'] = values[0][1]
            results['Wh_out'] = values[0][2]
            # if timestamp matches exactly, no interpolation req'd.
            interpolation_needed = not ((timestamp - values[0][0]) == 0)
        except:
            return {}

        first_time = values[0][0]
        cursor.execute('''
            SELECT * FROM metered
                WHERE timestamp > %d
                ORDER BY timestamp ASC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            second_time = values[0][0]

            if interpolation_needed:
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

    def _lookup_solar_data(self, timestamp):
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
        if values == []:
            return {}

        interpolation_needed = True
        # Find point #1
        try:
            # if timestamp matches exactly, no interpolation req'd.
            interpolation_needed = not ((timestamp - values[0][0]) == 0)
            results['Wh_gen'] = values[0][2]
        except:
            return {}

        # Find point #2
        first_time = values[0][0]
        cursor.execute('''
            SELECT * FROM system
                WHERE timestamp > %d
                ORDER BY timestamp ASC
                LIMIT 1
            ''' % timestamp)
        values = cursor.fetchall()
        if values == []:
            return {}

        try:
            second_time = values[0][0]

            if interpolation_needed:
                inter_gen = self._interpolate_value(
                    first_time, second_time,
                    results['Wh_gen'], values[0][2],
                )
                ts_diff = timestamp - first_time
                results['Wh_gen'] += inter_gen * ts_diff
        except:
            return {}

        if (second_time - first_time) < (self.INTERVAL * 3):
            try:
                # Temperature data
                cursor.execute('''
                    SELECT avg(Tdsp_degC), avg(Tmos_degC) FROM panels
                        WHERE (timestamp > %d) AND (timestamp <= %d)
                    ''' % (
                        first_time,
                        second_time,
                    ))
                values = cursor.fetchall()

                if values[0][1] is not None:
                    results['Cdsp_avg'] = values[0][1]
                if values[0][2] is not None:
                    results['Cmos_avg'] = values[0][2]
            except:
                pass

            try:
                # Voltage data
                cursor.execute('''
                    SELECT DISTINCT macrf FROM panels
                        WHERE (timestamp > %d) AND (timestamp <= %d)
                    ''' % (
                        first_time,
                        second_time,
                    ))
                values = cursor.fetchall()
                panels = []
                v_total = 0
                for v in values:
                    panels.append(v[0])
                for panel in panels:
                    cursor.execute('''
                        SELECT avg(Vin_V) FROM panels
                            WHERE (macrf = '%s') AND
                                (timestamp > %d) AND (timestamp <= %d)
                        ''' % (
                            panel,
                            first_time,
                            second_time,
                    ))
                    values = cursor.fetchall()
                    v_total += values[0][0]

                results['Vin_total'] = v_total
            except:
                pass

        cursor.close()
        db.close()

        return results

    def _get_solar_data(self, timestamp):
        results = self._lookup_solar_data(timestamp)
        if results == {}:
            return results

        previous_results = self._lookup_solar_data(timestamp - self.INTERVAL)
        if 'Wh_gen' in previous_results:
            results['prev_Wh_gen'] = previous_results['Wh_gen']

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
        return value[0][0] / float(self.WHCONVERT)

    def _calculate_pvoutput(self, timestamp, data):
        pvoutput = {}

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

        if (('prev_Wh_out' in data) and
            ('prev_Wh_in' in data) and
            ('prev_Wh_gen' in data)):
            data['prev_Wh_cons'] = data['prev_Wh_gen'] + data['prev_Wh_in'] - data['prev_Wh_out']
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

        if 'Vin_total' in data:
            pvoutput['v6'] = "%.1f" % data['Vin_total']
        if 'Cdsp_avg' in data:
            pvoutput['v7'] = "%.1f" % data['Cdsp_avg']
        if 'Cmos_avg' in data:
            pvoutput['v8'] = "%.1f" % data['Cmos_avg']

        # Remove once my metering actually works!! (and returns exported data)
        if (('v3' in pvoutput) and
            ('Wh_out' in data)):
            if data['Wh_out'] == 0:
                pvoutput['v3'] = "%.0f" % (
                    data['Wh_gen'] + data['Wh_in'] - self._fake_Wh_out(timestamp)
                )
            elif data['Wh_out'] != 0:
                pass
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

        if self.verbose:
            sys.stdout.write("%s" % time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp)))
            if 'Wh_in' in data:
                sys.stdout.write("; import=%dWh" % data['Wh_in'])
            if 'Wh_out' in data:
                sys.stdout.write("; export=%dWh" % data['Wh_out'])
            if 'v3' in pvoutput:
                sys.stdout.write("; consume=%dWh" % int(pvoutput['v3']))
            if 'v1' in pvoutput:
                sys.stdout.write("; produce=%dWh" % int(pvoutput['v1']))
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
            if remaining is None:
                return
            remaining = int(remaining)
        except Exception as e:
            print "ERROR: When quering API limit: %s" % str(e)

        if remaining <= 15:
            print "ERROR: less than 15 API calls remaining"
            return

        # Find stuff to upload
        self.cursor.execute('''
            SELECT * FROM pvoutput
                WHERE need_upload = 1
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
        t_end = int(time.time() - (20 * 60))

        for t in range(t_start, t_end):
            if (((int(time.strftime("%M", time.gmtime(t))) % self.MODULO) != 0) or
                ((int(time.strftime("%S", time.gmtime(t))) != 0))):
                continue
            # print time.strftime("%Y%m%d %H:%M", time.localtime(t))

            data = dict(
                self._get_meter_data(t).items() +
                self._get_solar_data(t).items()
            )

            pvoutput = self._calculate_pvoutput(t, data)
            if pvoutput is not None:
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
