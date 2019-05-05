import psutil
import subprocess

from threading import Lock

from igor import utils


_DEFAULT_WAIT_TIME = 10  # seconds
_SIGKILL = -9
_SIGTERM = -15
_SIGABORT = -6
_SIGFLT = -11
_SIGHUP = -1
_SIGINT = -2
_SIGWTF = 99999  # custom error code, translates to: I've no freakin' idea what happened
_MSG = {
    _SIGKILL: "killed (KILL)",
    _SIGTERM: "terminated (TERM)",
    _SIGABORT: "aborted (ABRT)",
    _SIGFLT: "segfault (SEGV)",
    _SIGHUP: "received sighup (HUP)",
    _SIGINT: "interrupted (INT)",
}


logger = utils.logger()


class ProcessManager:
    """Runs commands. Monitors running commands. Offers ability to kill running cmds by their
    job / layer / task id.

    """

    CODE_OK = "ok"
    CODE_KILLED = "killed"
    CODE_ERROR = "error"

    def __init__(self):
        self._lock = Lock()

        self._keys = {}  # key -> process ID
        self._values = {}  # process ID -> keys
        self._procs = {}  # process ID -> proc obj

    def _log_output(self, pipe_obj):
        """Send each line of output from the subprocess to the logger.

        :param pipe_obj: subprocess.stdout

        """
        for line in iter(pipe_obj.readline, b''):
            try:
                print(str(line, encoding='utf8'))
            except Exception as e:
                # don't stop just because we can't log a line
                logger.warn(f"unable to log: {e}: {line}")

    def run(self, keys, cmd, env=None) -> (int, str, str):
        """Run the given command & block until it completes.

        Returns (int, str, str) representing (exit_code, exitcode, message).

        The 'exitcode' above is a ProcessManager constant, one of:
            CODE_OK = "ok"
                The task exited successfully
            CODE_KILLED = "killed"
                The task did not exit successfully, but it was killed by us intentionally
            CODE_ERROR = "error"
                The task errored or otherwise exited unsuccessfully (eg. a non 0 exit exitcode
                and we did not kill it intentionally)

        Nb. a custom error exitcode of '99999' is returned if we've no idea what happened
        (hopefully the exception message reveals something :P).

        :param keys: key(s) that should be used to reference this cmd
        :param cmd: actual command to run
        :param env: env variables for the child process
        :returns: tuple

        """
        if env:
            # env values of None make Popen fall over :(
            env = {k: v for k, v in env.items() if k and v}

        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "env": env or None,
        }

        try:
            proc = psutil.Popen(cmd, **kwargs)
        except Exception as e:
            msg = str(e)

            try:
                return e.errno, self.CODE_ERROR, msg
            except AttributeError:
                return _SIGWTF, self.CODE_ERROR, msg

        process_id = proc.pid
        try:
            self._add(keys, proc)

            err_msg = ""
            msg = self.CODE_OK

            try:
                with proc.stdout:
                    self._log_output(proc.stdout)

                exitcode = proc.wait()

                if exitcode in [_SIGTERM, _SIGKILL]:
                    # We send TERM or KILL when we run self._kill(). Before that however we
                    # remove the process from our internal struct(s).
                    if not self._get_proc(process_id):
                        # it's KILLED here because WE killed it intentionally
                        # (hence why it wasn't found and raised TERM or KILL).
                        msg = self.CODE_KILLED
                        err_msg = f"process killed by manager ({exitcode})"
                    else:
                        err_msg = _MSG.get(exitcode, "")
                        msg = self.CODE_ERROR
                elif exitcode != 0:
                    msg = self.CODE_ERROR
                    err_msg = _MSG.get(exitcode, "")

            except Exception as e:
                msg = self.CODE_ERROR
                err_msg = str(e)

                try:
                    exitcode = e.errno
                except AttributeError:
                    exitcode = _SIGWTF

            return exitcode, msg, err_msg
        finally:
            self._remove(process_id)

    def kill_all(self, wait_time=_DEFAULT_WAIT_TIME):
        """Kill every process this manager looks after.

        :param wait_time: time to wait between SIGTERM and SIGKILL

        """
        with self._lock:
            for _, proc in self._procs.items():
                self._kill(proc, wait_time=wait_time)

            self._keys = {}
            self._values = {}
            self._procs = {}

    def _kill(self, proc, wait_time=_DEFAULT_WAIT_TIME):
        """Send a SIGKILL to the given process. If waittime > 0, SIGTERM is sent first, then
        SIGKILL only if the process is still alive after the wait time expires.

        :param proc: process obj
        :param wait_time:

        """
        self._remove(proc.pid)

        if wait_time > 0:
            # we're going to send TERM to the process. If it takes too long, we'll send KILL.
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                return

            try:
                proc.wait(timeout=wait_time)
            except psutil.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
            except psutil.NoSuchProcess:
                pass
        else:
            # we've been told not to be nice
            try:
                proc.kill()
            except Exception:
                pass

    def kill(self, key, wait_time=_DEFAULT_WAIT_TIME):
        """Kill the process for the given key, removing it from the manager.

        :param key:
        :param wait_time: time to wait between SIGTERM and SIGKILL

        """
        proc_id = self._get_pid(key)
        if not proc_id:
            return

        proc = self._get_proc(proc_id)
        if not proc:
            return

        self._kill(proc, wait_time=wait_time)

    def _status(self, process_id) -> dict:
        """Return the status of a given process id

        :param process_id:
        :return: dict

        """
        proc = self._get_proc(process_id)
        if not proc:
            return None

        try:
            raw = proc.as_dict()
        except psutil.NoSuchProcess:
            return None

        # TODO: It occurs to me that non *nix systems could have different values here.
        # Probably we should do something about this at some point. Or I mean, people could
        # just be encouraged to buy actual computers ..
        data = {
            "pid": proc.pid,
            "connections": len(raw.get("connections", [])),
            "open_files": len(raw.get("open_files", [])),
            "cpu_percent": raw.get("cpu_percent"),
            'status': raw.get('sleeping'),
            'username': raw.get("username"),
            'num_fds': raw.get("num_fds"),
            'num_threads': raw.get("num_threads"),
            'exe': raw.get("exe"),
            'cwd': raw.get("cwd"),
        }

        mem = raw.get("memory_full_info")
        if mem:
            data['mem_rss'] = mem.rss
            data['mem_vms'] = mem.vms
            data['mem_shared'] = mem.shared
            data['mem_text'] = mem.text
            data['mem_lib'] = mem.lib
            data['mem_data'] = mem.data
            data['mem_dirty'] = mem.dirty
            data['mem_uss'] = mem.uss
            data['mem_pss'] = mem.pss
            data['mem_swap'] = mem.swap

        return data

    def status(self, keys=None) -> list:
        """Return process data about all managed processes.

        If 'keys' is given, return only process data for the indicated keys.

        :param keys:
        :return: list

        """
        if keys:
            processes = [i for i in set(self._get_pid(k) for k in keys) if i]
        else:
            processes = self._managed_process_ids()

        result = []
        for p in processes:
            status = self._status(p)
            if not status:
                continue

            result.append(status)

        return result

    # protected funcs
    def _managed_process_ids(self) -> list:
        """Return a list of all processes IDs managed by this manager.

        :return: list

        """
        with self._lock:
            return list(self._values.keys())

    def _add(self, keys, proc):
        """Add keys -> value into the manager.

        :param keys: list of keys that should return 'value'
        :param proc: process obj

        """
        with self._lock:
            self._values[proc.pid] = keys
            self._procs[proc.pid] = proc

            for key in keys:
                self._keys[key] = proc.pid

    def _get_pid(self, key):
        """Return the process ID for this key (if known)

        :param key:

        """
        with self._lock:
            return self._keys.get(key)

    def _get_proc(self, process_id):
        """Return the process obj for this key (if known)

        :param process_id:

        """
        with self._lock:
            return self._procs.get(process_id)

    def _remove(self, proc_id):
        """Return all keys for the given proc_id

        :param proc_id:

        """
        with self._lock:
            for k in self._values[proc_id]:
                try:
                    del self._keys[k]
                except KeyError:
                    pass

            try:
                del self._procs[proc_id]
            except KeyError:
                pass

            try:
                del self._values[proc_id]
            except KeyError:
                pass