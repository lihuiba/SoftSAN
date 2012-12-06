/*
 * dmdebug.c
 *
 *  Created on: Nov 21, 2012
 *      Author: yongchuan
 */
#include <libdevmapper.h>

#define ARGHA _POSIX_C_SOURCE
#undef _POSIX_C_SOURCE
#include <Python.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/param.h>
#include <locale.h>
#include <langinfo.h>
#include <termios.h>

#include <fcntl.h>
#include <sys/stat.h>

#ifdef HAVE_GETOPTLONG
#  include <getopt.h>
#  define GETOPTLONG_FN(a, b, c, d, e) getopt_long((a), (b), (c), (d), (e))
#  define OPTIND_INIT 0
#else
struct option {
};
extern int optind;
extern char *optarg;
#  define GETOPTLONG_FN(a, b, c, d, e) getopt((a), (b), (c))
#  define OPTIND_INIT 1
#endif

#define LINE_SIZE 4096
#define ARGS_MAX 256
#define LOOP_TABLE_SIZE (PATH_MAX + 255)

#define DEFAULT_DM_DEV_DIR "/dev"

#define PYDM_ARGS (METH_VARARGS|METH_KEYWORDS)

enum {
	READ_ONLY = 0,
	COLS_ARG,
	EXEC_ARG,
	FORCE_ARG,
	GID_ARG,
	MAJOR_ARG,
	MINOR_ARG,
	MODE_ARG,
	NAMEPREFIXES_ARG,
	NOFLUSH_ARG,
	NOHEADINGS_ARG,
	NOLOCKFS_ARG,
	NOOPENCOUNT_ARG,
	NOTABLE_ARG,
	OPTIONS_ARG,
	READAHEAD_ARG,
	ROWS_ARG,
	SEPARATOR_ARG,
	SHOWKEYS_ARG,
	SORT_ARG,
	TABLE_ARG,
	TARGET_ARG,
	TREE_ARG,
	UID_ARG,
	UNBUFFERED_ARG,
	UNQUOTED_ARG,
	UUID_ARG,
	VERBOSE_ARG,
	VERSION_ARG,
	NUM_SWITCHES
};

typedef enum {
	DR_TASK = 1,
	DR_INFO = 2,
	DR_DEPS = 4,
	DR_TREE = 8	/* Complete dependency tree required */
} report_type_t;

static int _switches[NUM_SWITCHES];
static int _int_args[NUM_SWITCHES];
static char *_string_args[NUM_SWITCHES];
static int _num_devices;
static char *_uuid;
static char *_table;
static char *_target;
static char *_command;
static uint32_t _read_ahead_flags;
static struct dm_tree *_dtree;
static struct dm_report *_report;
static report_type_t _report_type;

typedef int (*command_fn) (int argc, char **argv, void *data);

struct command {
	const char *name;
	const char *help;
	int min_args;
	int max_args;
	command_fn fn;
};

struct dmsetup_report_obj {
	struct dm_task *task;
	struct dm_info *info;
	struct dm_task *deps_task;
	struct dm_tree_node *tree_node;
};

static struct dm_task *_get_deps_task(int major, int minor)
{
	struct dm_task *dmt;
	struct dm_info info;

	if (!(dmt = dm_task_create(DM_DEVICE_DEPS)))
		return NULL;

	if (!dm_task_set_major(dmt, major) ||
	    !dm_task_set_minor(dmt, minor))
		goto err;

	if (_switches[NOOPENCOUNT_ARG] && !dm_task_no_open_count(dmt))
		goto err;

	if (!dm_task_run(dmt))
		goto err;

	if (!dm_task_get_info(dmt, &info))
		goto err;

	if (!info.exists)
		goto err;

	return dmt;

      err:
	dm_task_destroy(dmt);
	return NULL;
}

static int _display_info_cols(struct dm_task *dmt, struct dm_info *info)
{
	struct dmsetup_report_obj obj;
	int r = 0;

	if (!info->exists) {
		fprintf(stderr, "Device does not exist.\n");
		return 0;
	}

	obj.task = dmt;
	obj.info = info;
	obj.deps_task = NULL;

	if (_report_type & DR_TREE)
		obj.tree_node = dm_tree_find_node(_dtree, info->major, info->minor);

	if (_report_type & DR_DEPS)
		obj.deps_task = _get_deps_task(info->major, info->minor);

	if (!dm_report_object(_report, &obj))
		goto out;

	r = 1;

      out:
	if (obj.deps_task)
		dm_task_destroy(obj.deps_task);
	return r;
}

static void _display_info_long(struct dm_task *dmt, struct dm_info *info)
{
	const char *uuid;
	uint32_t read_ahead;

	if (!info->exists) {
		printf("Device does not exist.\n");
		return;
	}

	printf("Name:              %s\n", dm_task_get_name(dmt));

	printf("State:             %s%s\n",
	       info->suspended ? "SUSPENDED" : "ACTIVE",
	       info->read_only ? " (READ-ONLY)" : "");

	/* FIXME Old value is being printed when it's being changed. */
	if (dm_task_get_read_ahead(dmt, &read_ahead))
		printf("Read Ahead:        %" PRIu32 "\n", read_ahead);

	if (!info->live_table && !info->inactive_table)
		printf("Tables present:    None\n");
	else
		printf("Tables present:    %s%s%s\n",
		       info->live_table ? "LIVE" : "",
		       info->live_table && info->inactive_table ? " & " : "",
		       info->inactive_table ? "INACTIVE" : "");

	if (info->open_count != -1)
		printf("Open count:        %d\n", info->open_count);

	printf("Event number:      %" PRIu32 "\n", info->event_nr);
	printf("Major, minor:      %d, %d\n", info->major, info->minor);

	if (info->target_count != -1)
		printf("Number of targets: %d\n", info->target_count);

	if ((uuid = dm_task_get_uuid(dmt)) && *uuid)
		printf("UUID: %s\n", uuid);

	printf("\n");
}

static int _display_info(struct dm_task *dmt)
{
	struct dm_info info;

	if (!dm_task_get_info(dmt, &info))
		return 0;

	if (!_switches[COLS_ARG])
		_display_info_long(dmt, &info);
	else
		/* FIXME return code */
		_display_info_cols(dmt, &info);

	return info.exists ? 1 : 0;
}

static int _parse_line(struct dm_task *dmt, char *buffer, const char *file,
		       int line)
{
	char ttype[LINE_SIZE], *ptr, *comment;
	unsigned long long start, size;
	int n;

	/* trim trailing space */
	for (ptr = buffer + strlen(buffer) - 1; ptr >= buffer; ptr--)
		if (!isspace((int) *ptr))
			break;
	ptr++;
	*ptr = '\0';

	/* trim leading space */
	for (ptr = buffer; *ptr && isspace((int) *ptr); ptr++)
		;

	if (!*ptr || *ptr == '#')
		return 1;

	if (sscanf(ptr, "%llu %llu %s %n",
		   &start, &size, ttype, &n) < 3) {
		err("Invalid format on line %d of table %s", line, file);
		return 0;
	}

	ptr += n;
	if ((comment = strchr(ptr, (int) '#')))
		*comment = '\0';

	if (!dm_task_add_target(dmt, start, size, ttype, ptr))
		return 0;

	return 1;
}

static int _parse_file(struct dm_task *dmt, const char *file)
{
	char *buffer = NULL;
	size_t buffer_size = 0;
	FILE *fp;
	int r = 0, line = 0;

	/* one-line table on cmdline */
	if (_table)
		return _parse_line(dmt, _table, "", ++line);

	/* OK for empty stdin */
	if (file) {
		if (!(fp = fopen(file, "r"))) {
			err("Couldn't open '%s' for reading", file);
			return 0;
		}
	} else
		fp = stdin;

#ifndef HAVE_GETLINE
	buffer_size = LINE_SIZE;
	if (!(buffer = dm_malloc(buffer_size))) {
		err("Failed to malloc line buffer.");
		return 0;
	}

	while (fgets(buffer, (int) buffer_size, fp))
#else
	while (getline(&buffer, &buffer_size, fp) > 0)
#endif
		if (!_parse_line(dmt, buffer, file ? : "on stdin", ++line))
			goto out;

	r = 1;

      out:
#ifndef HAVE_GETLINE
	dm_free(buffer);
#else
	free(buffer);
#endif
	if (file && fclose(fp))
		fprintf(stderr, "%s: fclose failed: %s", file, strerror(errno));

	return r;
}

//set a dmtask's name, it can be a pair of (major, minor), device name, or device's uuid
static int _set_task_device(struct dm_task *dmt, const char *name, int optional)
{
	if (name) {
		if (!dm_task_set_name(dmt, name))
			return 0;
	} else if (_switches[UUID_ARG]) {
		if (!dm_task_set_uuid(dmt, _uuid))
			return 0;
	} else if (_switches[MAJOR_ARG] && _switches[MINOR_ARG]) {
		if (!dm_task_set_major(dmt, _int_args[MAJOR_ARG]) ||
		    !dm_task_set_minor(dmt, _int_args[MINOR_ARG]))
			return 0;
	} else if (!optional) {
		fprintf(stderr, "No device specified.\n");
		return 0;
	}

	return 1;
}

//get a device's size, in unit of 512 byte
static uint64_t _get_device_size(const char *name)
{
	uint64_t start, length, size = UINT64_C(0);
	struct dm_info info;
	char *target_type, *params;
	struct dm_task *dmt;
	void *next = NULL;

	if (!(dmt = dm_task_create(DM_DEVICE_TABLE)))
		return 0;

	if (!_set_task_device(dmt, name, 0))
		goto out;

	if (_switches[NOOPENCOUNT_ARG] && !dm_task_no_open_count(dmt))
		goto out;

	if (!dm_task_run(dmt))
		goto out;

	if (!dm_task_get_info(dmt, &info) || !info.exists)
		goto out;

	do {
		next = dm_get_next_target(dmt, next, &start, &length,
					  &target_type, &params);
		size += length;
	} while (next);

      out:
	dm_task_destroy(dmt);
	return size;
}

//Parse the command arguments
static int _process_switches(int *argc, char ***argv, const char *dev_dir)
{
	char *base, *namebase, *s;
	static int ind;
	int c, r;

#ifdef HAVE_GETOPTLONG
	static struct option long_options[] = {
		{"readonly", 0, &ind, READ_ONLY},
		{"columns", 0, &ind, COLS_ARG},
		{"exec", 1, &ind, EXEC_ARG},
		{"force", 0, &ind, FORCE_ARG},
		{"gid", 1, &ind, GID_ARG},
		{"major", 1, &ind, MAJOR_ARG},
		{"minor", 1, &ind, MINOR_ARG},
		{"mode", 1, &ind, MODE_ARG},
		{"nameprefixes", 0, &ind, NAMEPREFIXES_ARG},
		{"noflush", 0, &ind, NOFLUSH_ARG},
		{"noheadings", 0, &ind, NOHEADINGS_ARG},
		{"nolockfs", 0, &ind, NOLOCKFS_ARG},
		{"noopencount", 0, &ind, NOOPENCOUNT_ARG},
		{"notable", 0, &ind, NOTABLE_ARG},
		{"options", 1, &ind, OPTIONS_ARG},
		{"readahead", 1, &ind, READAHEAD_ARG},
		{"rows", 0, &ind, ROWS_ARG},
		{"separator", 1, &ind, SEPARATOR_ARG},
		{"showkeys", 0, &ind, SHOWKEYS_ARG},
		{"sort", 1, &ind, SORT_ARG},
		{"table", 1, &ind, TABLE_ARG},
		{"target", 1, &ind, TARGET_ARG},
		{"tree", 0, &ind, TREE_ARG},
		{"uid", 1, &ind, UID_ARG},
		{"uuid", 1, &ind, UUID_ARG},
		{"unbuffered", 0, &ind, UNBUFFERED_ARG},
		{"unquoted", 0, &ind, UNQUOTED_ARG},
		{"verbose", 1, &ind, VERBOSE_ARG},
		{"version", 0, &ind, VERSION_ARG},
		{0, 0, 0, 0}
	};
#else
	struct option long_options;
#endif

	/*
	 * Zero all the index counts.
	 */
	memset(&_switches, 0, sizeof(_switches));
	memset(&_int_args, 0, sizeof(_int_args));
	_read_ahead_flags = 0;

	namebase = strdup((*argv)[0]);
	base = basename(namebase);

	if (!strcmp(base, "devmap_name")) {
		free(namebase);
		_switches[COLS_ARG]++;
		_switches[NOHEADINGS_ARG]++;
		_switches[OPTIONS_ARG]++;
		_switches[MAJOR_ARG]++;
		_switches[MINOR_ARG]++;
		_string_args[OPTIONS_ARG] = (char *) "name";

		if (*argc == 3) {
			_int_args[MAJOR_ARG] = atoi((*argv)[1]);
			_int_args[MINOR_ARG] = atoi((*argv)[2]);
			*argc -= 2;
			*argv += 2;
		} else if ((*argc == 2) &&
			   (2 == sscanf((*argv)[1], "%i:%i",
					&_int_args[MAJOR_ARG],
					&_int_args[MINOR_ARG]))) {
			*argc -= 1;
			*argv += 1;
		} else {
			fprintf(stderr, "Usage: devmap_name <major> <minor>\n");
			return 0;
		}

		(*argv)[0] = (char *) "info";
		return 1;
	}

	if (!strcmp(base, "losetup") || !strcmp(base, "dmlosetup")){
		//r = _process_losetup_switches(base, argc, argv, dev_dir);
		free(namebase);
		return r;
	}

	free(namebase);

	optarg = 0;
	optind = OPTIND_INIT;
	while ((ind = -1, c = GETOPTLONG_FN(*argc, *argv, "cCfGj:m:Mno:O:ru:Uv",
					    long_options, NULL)) != -1) {
		if (c == ':' || c == '?')
			return 0;
		if (c == 'c' || c == 'C' || ind == COLS_ARG)
			_switches[COLS_ARG]++;
		if (c == 'f' || ind == FORCE_ARG)
			_switches[FORCE_ARG]++;
		if (c == 'r' || ind == READ_ONLY)
			_switches[READ_ONLY]++;
		if (c == 'j' || ind == MAJOR_ARG) {
			_switches[MAJOR_ARG]++;
			_int_args[MAJOR_ARG] = atoi(optarg);
		}
		if (c == 'm' || ind == MINOR_ARG) {
			_switches[MINOR_ARG]++;
			_int_args[MINOR_ARG] = atoi(optarg);
		}
		if (c == 'n' || ind == NOTABLE_ARG)
			_switches[NOTABLE_ARG]++;
		if (c == 'o' || ind == OPTIONS_ARG) {
			_switches[OPTIONS_ARG]++;
			_string_args[OPTIONS_ARG] = optarg;
		}
		if (ind == SEPARATOR_ARG) {
			_switches[SEPARATOR_ARG]++;
			_string_args[SEPARATOR_ARG] = optarg;
		}
		if (c == 'O' || ind == SORT_ARG) {
			_switches[SORT_ARG]++;
			_string_args[SORT_ARG] = optarg;
		}
		if (c == 'v' || ind == VERBOSE_ARG)
			_switches[VERBOSE_ARG]++;
		if (c == 'u' || ind == UUID_ARG) {
			_switches[UUID_ARG]++;
			_uuid = optarg;
		}
		if (c == 'G' || ind == GID_ARG) {
			_switches[GID_ARG]++;
			_int_args[GID_ARG] = atoi(optarg);
		}
		if (c == 'U' || ind == UID_ARG) {
			_switches[UID_ARG]++;
			_int_args[UID_ARG] = atoi(optarg);
		}
		if (c == 'M' || ind == MODE_ARG) {
			_switches[MODE_ARG]++;
			/* FIXME Accept modes as per chmod */
			_int_args[MODE_ARG] = (int) strtol(optarg, NULL, 8);
		}
		if ((ind == EXEC_ARG)) {
			_switches[EXEC_ARG]++;
			_command = optarg;
		}
		if ((ind == TARGET_ARG)) {
			_switches[TARGET_ARG]++;
			_target = optarg;
		}
		if ((ind == NAMEPREFIXES_ARG))
			_switches[NAMEPREFIXES_ARG]++;
		if ((ind == NOFLUSH_ARG))
			_switches[NOFLUSH_ARG]++;
		if ((ind == NOHEADINGS_ARG))
			_switches[NOHEADINGS_ARG]++;
		if ((ind == NOLOCKFS_ARG))
			_switches[NOLOCKFS_ARG]++;
		if ((ind == NOOPENCOUNT_ARG))
			_switches[NOOPENCOUNT_ARG]++;
		if ((ind == READAHEAD_ARG)) {
			_switches[READAHEAD_ARG]++;
			if (!strcasecmp(optarg, "auto"))
				_int_args[READAHEAD_ARG] = DM_READ_AHEAD_AUTO;
			else if (!strcasecmp(optarg, "none"))
                		_int_args[READAHEAD_ARG] = DM_READ_AHEAD_NONE;
			else {
				for (s = optarg; isspace(*s); s++)
					;
				if (*s == '+')
					_read_ahead_flags = DM_READ_AHEAD_MINIMUM_FLAG;
				_int_args[READAHEAD_ARG] = atoi(optarg);
				if (_int_args[READAHEAD_ARG] < -1) {
//					log_error("Negative read ahead value "
//						  "(%d) is not understood.",
//						  _int_args[READAHEAD_ARG]);
					return 0;
				}
			}
		}
		if ((ind == ROWS_ARG))
			_switches[ROWS_ARG]++;
		if ((ind == SHOWKEYS_ARG))
			_switches[SHOWKEYS_ARG]++;
		if ((ind == TABLE_ARG)) {
			_switches[TABLE_ARG]++;
			_table = optarg;
		}
		if ((ind == TREE_ARG))
			_switches[TREE_ARG]++;
		if ((ind == UNQUOTED_ARG))
			_switches[UNQUOTED_ARG]++;
		if ((ind == VERSION_ARG))
			_switches[VERSION_ARG]++;
	}

	if (_switches[VERBOSE_ARG] > 1)
		dm_log_init_verbose(_switches[VERBOSE_ARG] - 1);

	if ((_switches[MAJOR_ARG] && !_switches[MINOR_ARG]) ||
	    (!_switches[MAJOR_ARG] && _switches[MINOR_ARG])) {
		fprintf(stderr, "Please specify both major number and "
				"minor number.\n");
		return 0;
	}

	/*if (_switches[TREE_ARG] && !_process_tree_options(_string_args[OPTIONS_ARG]))
		return 0;*/

	if (_switches[TABLE_ARG] && _switches[NOTABLE_ARG]) {
		fprintf(stderr, "--table and --notable are incompatible.\n");
		return 0;
	}

	*argv += optind;
	*argc -= optind;
	return 1;
}

static int _error_device(int argc __attribute((unused)), char **argv __attribute((unused)), void *data)
{
	struct dm_names *names = (struct dm_names *) data;
	struct dm_task *dmt;
	const char *name;
	uint64_t size;
	int r = 0;

	if (data)
		name = names->name;
	else
		name = argv[1];

	size = _get_device_size(name);

	if (!(dmt = dm_task_create(DM_DEVICE_RELOAD)))
		return 0;

	if (!_set_task_device(dmt, name, 0))
		goto error;

	if (!dm_task_add_target(dmt, UINT64_C(0), size, "error", ""))
		goto error;

	if (_switches[READ_ONLY] && !dm_task_set_ro(dmt))
		goto error;

	if (_switches[NOOPENCOUNT_ARG] && !dm_task_no_open_count(dmt))
		goto error;

	if (!dm_task_run(dmt))
		goto error;

	if (!_simple(DM_DEVICE_RESUME, name, 0, 0)) {
		_simple(DM_DEVICE_CLEAR, name, 0, 0);
		goto error;
	}

	r = 1;

error:
	dm_task_destroy(dmt);
	return r;
}

int _simple(int task, const char *name, uint32_t event_nr, int display)
{
	int r = 0;

	struct dm_task *dmt;

	if (!(dmt = dm_task_create(task)))
		return 0;

	if (!_set_task_device(dmt, name, 0))
		goto out;

	if (event_nr && !dm_task_set_event_nr(dmt, event_nr))
		goto out;

	if (_switches[NOFLUSH_ARG] && !dm_task_no_flush(dmt))
		goto out;

	if (_switches[NOOPENCOUNT_ARG] && !dm_task_no_open_count(dmt))
		goto out;

	if (_switches[NOLOCKFS_ARG] && !dm_task_skip_lockfs(dmt))
		goto out;

	if (_switches[READAHEAD_ARG] &&
	    !dm_task_set_read_ahead(dmt, _int_args[READAHEAD_ARG],
				    _read_ahead_flags))
		goto out;

	r = dm_task_run(dmt);

	if (r && display && _switches[VERBOSE_ARG])
		r = _display_info(dmt);

      out:
	dm_task_destroy(dmt);
	return r;
}

static int _create(int argc, char **argv, void *data __attribute((unused)))
{
	int r = 0;
	struct dm_task *dmt;
	const char *file = NULL;

	if (argc == 3)
		file = argv[2];

	if (!(dmt = dm_task_create(DM_DEVICE_CREATE)))
		return 0;

	if (!dm_task_set_name(dmt, argv[1]))
		goto out;

	if (_switches[UUID_ARG] && !dm_task_set_uuid(dmt, _uuid))
		goto out;

	if (!_switches[NOTABLE_ARG] && !_parse_file(dmt, file))
		goto out;

	if (_switches[READ_ONLY] && !dm_task_set_ro(dmt))
		goto out;

	if (_switches[MAJOR_ARG] && !dm_task_set_major(dmt, _int_args[MAJOR_ARG]))
		goto out;

	if (_switches[MINOR_ARG] && !dm_task_set_minor(dmt, _int_args[MINOR_ARG]))
		goto out;

	if (_switches[UID_ARG] && !dm_task_set_uid(dmt, _int_args[UID_ARG]))
		goto out;

	if (_switches[GID_ARG] && !dm_task_set_gid(dmt, _int_args[GID_ARG]))
		goto out;

	if (_switches[MODE_ARG] && !dm_task_set_mode(dmt, _int_args[MODE_ARG]))
		goto out;

	if (_switches[NOOPENCOUNT_ARG] && !dm_task_no_open_count(dmt))
		goto out;

	if (_switches[READAHEAD_ARG] &&
	    !dm_task_set_read_ahead(dmt, _int_args[READAHEAD_ARG],
				    _read_ahead_flags))
		goto out;

	if (!dm_task_run(dmt))
		goto out;

	r = 1;

//	if (_switches[VERBOSE_ARG])
//		r = _display_info(dmt);

      out:
	dm_task_destroy(dmt);

	return r;
}

static int _remove(int argc, char **argv, void *data __attribute((unused)))
{
	int r;

	if (_switches[FORCE_ARG] && argc > 1)
		r = _error_device(argc, argv, NULL);

	return _simple(DM_DEVICE_REMOVE, argc > 1 ? argv[1] : NULL, 0, 0);
}

//get a device's info, now only basic info
static int _info(int argc, char **argv, void *data __attribute((unused)))
{
	int r = 0;
	char *name = argv[2];
	struct dm_task *dmt;
	struct dm_info info;

	dmt = dm_task_create(DM_DEVICE_INFO);
	_set_task_device(dmt, name, 0);
	dm_task_run(dmt);

	dm_task_get_info(dmt, &info);
	//printf("device name is:----%s\n", dm_task_get_name(dmt));

	return r;
}

//find a command
static struct command _commands[] = {
		{"create", "<device_name> ", 1, 2, _create},
		{"remove", "<device_name>", 0, 1, _remove},
		//{"info", "<device_name>", 0, 1, _info},
		{NULL, NULL, 0, 0, NULL}
};

static void _usage(FILE *out)
{
	fprintf(out, "Usage:\n\n");
	fprintf(out, "create <device_name>  <mapping_table_file>\n"
		"remove <device_name>\n"
		"info   <device_name>\n");
	fprintf(out, "\n");
	return;
}

static struct command *_find_command(const char *name)
{
	int i;

	for (i = 0; _commands[i].name; i++)
		if (!strcmp(_commands[i].name, name))
			return _commands + i;

	return NULL;
}

//to execute a dmsetup command, you need these data:argument number -- argc, argument array -- argv, and set the mark array _switches[] which
//can be processed while execute a dmsetup command, or you can set it manually(not avaible now).
static int execute_c(int argc, char **argv)//this function is used to execute a C function
{
	//printf("--------------------------Point 0------------------------------------");

	struct command *c;
	int r = 1;
	const char *dev_dir;

	(void) setlocale(LC_ALL, "");

	dev_dir = getenv ("DM_DEV_DIR");
	if (dev_dir && *dev_dir) {
		if (!dm_set_dev_dir(dev_dir)) {
			fprintf(stderr, "Invalid DM_DEV_DIR environment variable value.\n");
			goto out;
		}
	} else
		dev_dir = DEFAULT_DM_DEV_DIR;

	//printf("--------------------------Point 1------------------------------------");

	if (!_process_switches(&argc, &argv, dev_dir)) {
		fprintf(stderr, "Couldn't process command line.\n");
		goto out;
	}

	//printf("--------------------------Point 2------------------------------------");

	if (_switches[VERSION_ARG]) {
		c = _find_command("version");
		goto doit;
	}

	if (argc == 0) {
		_usage(stderr);
		goto out;
	}

	//printf("--------------------------Point 3------------------------------------");

	if (!(c = _find_command(argv[0]))) {
		fprintf(stderr, "Unknown command\n");
		_usage(stderr);
		goto out;
	}

	//printf("--------------------------Point 4------------------------------------");

	if (argc < c->min_args + 1 ||
		(c->max_args >= 0 && argc > c->max_args + 1)) {
		fprintf(stderr, "Incorrect number of arguments\n");
		_usage(stderr);
		goto out;
	}

	//if (_switches[COLS_ARG] && !_report_init(c))
		//goto out;

	  doit:

	if (!c->fn(argc, argv, NULL)) {
		fprintf(stderr, "Command failed\n");
		goto out;
	}

	r = 0;

out:
	if (_report) {
		dm_report_output(_report);
		dm_report_free(_report);
	}

	if (_dtree)
		dm_tree_free(_dtree);

	return 0;
}

//get a command from python, convert it into C data structure and execute this command
PyObject * Get_Command(PyObject *self, PyObject *args)
{
//--for test :-------------------------------------------------------
//-------------------------------------------------------------------

	int argc;
	int ret;
	int i;
	char *argv[100];
	PyObject *list;
	PyObject *pystring;
	Py_ssize_t listlen;

	ret = PyArg_ParseTuple(args, "O!", &PyList_Type, &list);
	if( -1 == ret )
	{
		printf("Get_Command: Parse arguments failed!\n");
		return Py_BuildValue("i", 0);
	}

	argv[0] = (char *)malloc(sizeof(char[100]));
	strcpy(argv[0], "dm");
	listlen = PyList_Size(list);
	argc = listlen;
	for(i = 0; i < argc; i++)
	{
		argv[i+1] = (char *)malloc(sizeof(char[100]));
		pystring = PyList_GetItem(list, i);
		strcpy(argv[i+1], PyString_AsString(pystring));
	}

	if( !strcmp(argv[1], "info") )
		_info(argc+1, argv, NULL);
	else
		execute_c(argc+1, argv);

	return Py_BuildValue("i", 1);
}

//Define a methodlist which can be used in python, {NULL, NULL} means end
static PyMethodDef dmfunctions[] = {
	{"Get_Command", (PyCFunction)Get_Command, PYDM_ARGS, "get and execute a python command"},
	{NULL, NULL}
};

//Initial a python module that can be used in python
void initdm(void)
{
	PyObject *m;

	m = Py_InitModule("dm", dmfunctions);
}

int main()
{
	return 0;
}