import logging
import platform
import os

logger = logging.getLogger(__name__)

class SystemInformation():
    def __init__(self, db=None):
        self.system = platform.system()
        self.db = db

    @property
    def on_heroku(self):
        """ Are we running on heroku?"""
        return 'DYNO' in os.environ

    def os(self):
        osinfo = {}
        osinfo['system'] = platform.system()

        if self.system == 'Linux':
            (osinfo['distribution'], osinfo['distribution_version']) = platform.linux_distribution()[0:2]
        elif self.system == 'Darwin':
            osinfo['distribution'] = 'OS X'
            osinfo['distribution_version'] = platform.mac_ver()[0]

        osinfo['architecture'] = platform.machine()
        osinfo['kernel_version'] = platform.release()

        try:
            with open('/sys/devices/virtual/dmi/id/sys_vendor', 'r') as f:
                vendor = f.readline().strip()
            with open('/sys/devices/virtual/dmi/id/product_name', 'r') as f:
                model = f.readline().strip()
            if vendor and model:
                osinfo['server_model'] = "%s %s" % (vendor, model)
        except Exception as e:
            logger.debug("Error while collecting sys_vendor/product_name from sysfs: %s" % e)

        return osinfo

    def cpu(self):
        result = {}

        if self.system == 'Linux':
            (procstat, cpuinfo) = self._fetch_linux_cpu_data()

            result['busy_times'] = self._parse_linux_cpu_procstat(procstat)
            result['hardware'] = self._parse_linux_cpu_cpuinfo(cpuinfo)

        else:
            return None

        return (result)

    @staticmethod
    def _fetch_linux_cpu_data():

        with open('/proc/stat', 'r') as f:
            procstat = f.readlines()

        with open('/proc/cpuinfo', 'r') as f:
            cpuinfo = f.readlines()

        return procstat, cpuinfo

    @staticmethod
    def _parse_linux_cpu_procstat(procstat):

        # Fetch combined CPU counter from lines
        os_counters = filter(lambda x: x.find('cpu ') == 0, procstat)[0]

        # tokenize, strip row heading
        os_counters = os_counters.split()[1:]

        # Correct all values to msec
        kernel_hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        os_counters = map(lambda x: int(x) * (1000 / kernel_hz), os_counters)

        os_counter_names = ['user_msec', 'nice_msec', 'system_msec', 'idle_msec', 'iowait_msec',
                            'irq_msec', 'softirq_msec', 'steal_msec', 'guest_msec', 'guest_nice_msec']

        return dict(zip(os_counter_names, os_counters))

    @staticmethod
    def _parse_linux_cpu_cpuinfo(cpuinfo):

        # Trim excessive whitespace in strings, return two elements per line
        cpuinfo = map(lambda x: " ".join(x.split()).split(' : '), cpuinfo)

        hardware = {}
        hardware['model'] = next(l[1] for l in cpuinfo if l[0] == 'model name')
        hardware['cache_size'] = next(l[1] for l in cpuinfo if l[0] == 'cache size')
        hardware['speed_mhz'] = next(round(float(l[1]), 2) for l in cpuinfo if l[0] == 'cpu MHz')

        try:
            hardware['sockets'] = int(max([l[1] for l in cpuinfo if l[0] == 'physical id'])) + 1
        except ValueError:
            # Fallthrough - we didn't find any physical id stanza, assuming one socket
            hardware['sockets'] = 1

        try:
            hardware['cores_per_socket'] = next(int(l[1]) for l in cpuinfo if l[0] == 'cpu cores')
        except StopIteration:
            # Fallthrough - we didn't find cpu cores stanza
            pass

        # We didn't get cpu core identifiers, just use the count of processors
        if not 'cores_per_socket' in hardware:
            try:
                hardware['cores_per_socket'] = int(max([l[1] for l in cpuinfo if l[0] == 'processor'])) + 1
            except ValueError:
                # All bets are off
                hardware['cores_per_socket'] = 1

        return hardware

    def scheduler(self):
        result = {}
        if self.system != 'Linux': return None

        with open('/proc/stat', 'r') as f:
            os_counters = f.readlines()

        os_counters = [l.split() for l in os_counters if len(l) > 1]

        result['interrupts'] = next(int(l[1]) for l in os_counters if l[0] == 'intr')
        result['context_switches'] = next(int(l[1]) for l in os_counters if l[0] == 'ctxt')
        result['procs_running'] = next(int(l[1]) for l in os_counters if l[0] == 'procs_running')
        result['procs_blocked'] = next(int(l[1]) for l in os_counters if l[0] == 'procs_blocked')
        result['procs_created'] = next(int(l[1]) for l in os_counters if l[0] == 'processes')

        with open('/proc/loadavg', 'r') as f:
            loadavg = f.readlines()

        loadavg = map(lambda x: float(x), loadavg[0].split()[:3])

        result['loadavg_1min'] = loadavg[0]
        result['loadavg_5min'] = loadavg[1]
        result['loadavg_15min'] = loadavg[2]

        return result

    def storage(self):
        result = {}

        if self.system != 'Linux':
            return None

        # FIXME: Collect information for all tablespaces and pg_xlog

        try:
            data_directory = self.db.run_query('SHOW data_directory', should_raise=True)[0]['data_directory']
        except Exception as e:
            logger.debug("Failure: %s, skipping storage data", str(e))
            return None

        result['name'] = 'PGDATA directory'
        result['path'] = data_directory
        result['mountpoint'] = self._find_mount_point(data_directory)

        vfs_stats = os.statvfs(data_directory)

        result['bytes_total'] = vfs_stats.f_bsize * vfs_stats.f_blocks
        result['bytes_available'] = vfs_stats.f_bsize * vfs_stats.f_bavail

        devicenode = os.stat(data_directory).st_dev
        major = os.major(devicenode)
        minor = os.minor(devicenode)

        sysfs_device_path = "/sys/dev/block/%d:%d/" % (major, minor)

        # not all devices have stats
        if os.path.exists(sysfs_device_path + 'stat'):
            with open(sysfs_device_path + 'stat', 'r') as f:
                device_stats = map(int, f.readline().split())

            stat_fields = ['rd_ios', 'rd_merges', 'rd_sectors', 'rd_ticks',
                           'wr_ios', 'wr_merges', 'wr_sectors', 'wr_ticks',
                           'ios_in_prog', 'tot_ticks', 'rq_ticks']

            result['perfdata'] = dict(zip(stat_fields, device_stats))

        # Vendor/Model doesn't exist for metadevices
        if os.path.exists(sysfs_device_path + 'device/vendor'):
            with open(sysfs_device_path + 'device/vendor', 'r') as f:
                vendor = f.readline().strip()

            with open(sysfs_device_path + 'device/model', 'r') as f:
                model = f.readline().strip()

            result['hardware'] = " ".join([vendor, model])

        return [result]

    def memory(self):
        result = {}

        if self.system != 'Linux': return None

        with open('/proc/meminfo') as f:
            meminfo = f.readlines()

        # Strip whitespace, drop kb suffix, split into two elements
        meminfo = dict(map(lambda x: " ".join(x.split()[:2]).split(': '), meminfo))

        # Initialize missing fields (openvz et al), convert to bytes
        for k in ['MemTotal', 'MemFree', 'Buffers', 'Cached', 'SwapTotal', 'SwapFree', 'Dirty', 'Writeback']:
            if not meminfo.get(k):
                meminfo[k] = 0
            else:
                meminfo[k] = int(meminfo[k]) * 1024

        result['total_bytes'] = meminfo['MemTotal']
        result['buffers_bytes'] = meminfo['Buffers']
        result['pagecache_bytes'] = meminfo['Cached']
        result['free_bytes'] = meminfo['MemFree']
        result['applications_bytes'] = meminfo['MemTotal'] - meminfo['MemFree'] - meminfo['Buffers'] - meminfo['Cached']
        result['dirty_bytes'] = meminfo['Dirty']
        result['writeback_bytes'] = meminfo['Writeback']
        result['swap_total_bytes'] = meminfo['SwapTotal']
        result['swap_free_bytes'] = meminfo['SwapFree']

        return result

    @staticmethod
    def _find_mount_point(path):
        path = os.path.abspath(path)
        while not os.path.ismount(path):
            path = os.path.dirname(path)
        return path


def find_executable_in_path(cmd):
    for path in os.environ['PATH'].split(os.pathsep):
        test = "%s/%s" % (path, cmd)
        logger.debug("Testing %s" % test)
        if os.path.isfile(test) and os.access(test, os.X_OK):
            return test
    return None
