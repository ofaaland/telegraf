/*
Lustre 2.x telegraf plugin

Lustre (http://lustre.org/) is an open-source, parallel file system
for HPC environments. It stores statistics about its activity in
/proc

*/
package lustre2

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type statSource struct {
	target   string // Lustre target which reported the data (e.g. fsname-OST0003)
	jobid    string // valid if non-zero
}

// Lustre proc files can change between versions, so we want to future-proof
// by letting people choose what to look at.
type Lustre2 struct {
	Ost_procfiles []string
	Mds_procfiles []string

	// Each mapping records a set of desired fields and how to find them
	wanted_maps map[string]map[bool][]*mapping

	// record metric fields and their origin
	// allFields[target="lquake-OST0000",jobid=""][field-name] := field-value
	// allFields[target="lquake-OST0000",jobid="opal-3334"][field-name] := field-value
	allFields map[statSource]map[string]interface{}
}

var sampleConfig = `
  ## An array of /proc globs to search for Lustre stats
  ## If not specified, the default will work on Lustre 2.5.x
  ##
  # ost_procfiles = [
  #   "/proc/fs/lustre/obdfilter/*/stats",
  #   "/proc/fs/lustre/osd-ldiskfs/*/stats",
  #   "/proc/fs/lustre/obdfilter/*/job_stats",
  # ]
  # mds_procfiles = [
  #   "/proc/fs/lustre/mdt/*/md_stats",
  #   "/proc/fs/lustre/mdt/*/job_stats",
  # ]
`

/* The wanted fields would be a []string if not for the
lines that start with read_bytes/write_bytes and contain
   both the byte count and the function call count
*/
type mapping struct {
	inProc   string // What to look for at the start of a line in /proc/fs/lustre/*
	field    uint32 // which field to extract from that line
	reportAs string // What measurement name to use
	tag      string // Additional tag to add for this metric
}

var wanted_ost_fields = []*mapping{
	{
		inProc:   "write_bytes",
		field:    6,
		reportAs: "write_bytes",
	},
	{ // line starts with 'write_bytes', but value write_calls is in second column
		inProc:   "write_bytes",
		field:    1,
		reportAs: "write_calls",
	},
	{
		inProc:   "read_bytes",
		field:    6,
		reportAs: "read_bytes",
	},
	{ // line starts with 'read_bytes', but value read_calls is in second column
		inProc:   "read_bytes",
		field:    1,
		reportAs: "read_calls",
	},
	{
		inProc: "cache_hit",
	},
	{
		inProc: "cache_miss",
	},
	{
		inProc: "cache_access",
	},
}

var wanted_ost_jobstats_fields = []*mapping{
	{ // The read line has several fields, so we need to differentiate what they are
		inProc:   "read",
		field:    3,
		reportAs: "jobstats_read_calls",
	},
	{
		inProc:   "read",
		field:    7,
		reportAs: "jobstats_read_min_size",
	},
	{
		inProc:   "read",
		field:    9,
		reportAs: "jobstats_read_max_size",
	},
	{
		inProc:   "read",
		field:    11,
		reportAs: "jobstats_read_bytes",
	},
	{ // Different inProc for newer versions
		inProc:   "read_bytes",
		field:    3,
		reportAs: "jobstats_read_calls",
	},
	{
		inProc:   "read_bytes",
		field:    7,
		reportAs: "jobstats_read_min_size",
	},
	{
		inProc:   "read_bytes",
		field:    9,
		reportAs: "jobstats_read_max_size",
	},
	{
		inProc:   "read_bytes",
		field:    11,
		reportAs: "jobstats_read_bytes",
	},
	{ // We need to do the same for the write fields
		inProc:   "write",
		field:    3,
		reportAs: "jobstats_write_calls",
	},
	{
		inProc:   "write",
		field:    7,
		reportAs: "jobstats_write_min_size",
	},
	{
		inProc:   "write",
		field:    9,
		reportAs: "jobstats_write_max_size",
	},
	{
		inProc:   "write",
		field:    11,
		reportAs: "jobstats_write_bytes",
	},
	{ // Different inProc for newer versions
		inProc:   "write_bytes",
		field:    3,
		reportAs: "jobstats_write_calls",
	},
	{
		inProc:   "write_bytes",
		field:    7,
		reportAs: "jobstats_write_min_size",
	},
	{
		inProc:   "write_bytes",
		field:    9,
		reportAs: "jobstats_write_max_size",
	},
	{
		inProc:   "write_bytes",
		field:    11,
		reportAs: "jobstats_write_bytes",
	},
	{
		inProc:   "getattr",
		field:    3,
		reportAs: "jobstats_ost_getattr",
	},
	{
		inProc:   "setattr",
		field:    3,
		reportAs: "jobstats_ost_setattr",
	},
	{
		inProc:   "punch",
		field:    3,
		reportAs: "jobstats_punch",
	},
	{
		inProc:   "sync",
		field:    3,
		reportAs: "jobstats_ost_sync",
	},
	{
		inProc:   "destroy",
		field:    3,
		reportAs: "jobstats_destroy",
	},
	{
		inProc:   "create",
		field:    3,
		reportAs: "jobstats_create",
	},
	{
		inProc:   "statfs",
		field:    3,
		reportAs: "jobstats_ost_statfs",
	},
	{
		inProc:   "get_info",
		field:    3,
		reportAs: "jobstats_get_info",
	},
	{
		inProc:   "set_info",
		field:    3,
		reportAs: "jobstats_set_info",
	},
	{
		inProc:   "quotactl",
		field:    3,
		reportAs: "jobstats_quotactl",
	},
}

var wanted_mds_fields = []*mapping{
	{
		inProc: "open",
	},
	{
		inProc: "close",
	},
	{
		inProc: "mknod",
	},
	{
		inProc: "link",
	},
	{
		inProc: "unlink",
	},
	{
		inProc: "mkdir",
	},
	{
		inProc: "rmdir",
	},
	{
		inProc: "rename",
	},
	{
		inProc: "getattr",
	},
	{
		inProc: "setattr",
	},
	{
		inProc: "getxattr",
	},
	{
		inProc: "setxattr",
	},
	{
		inProc: "statfs",
	},
	{
		inProc: "sync",
	},
	{
		inProc: "samedir_rename",
	},
	{
		inProc: "crossdir_rename",
	},
}

var wanted_mdt_jobstats_fields = []*mapping{
	{
		inProc:   "open",
		field:    3,
		reportAs: "jobstats_open",
	},
	{
		inProc:   "close",
		field:    3,
		reportAs: "jobstats_close",
	},
	{
		inProc:   "mknod",
		field:    3,
		reportAs: "jobstats_mknod",
	},
	{
		inProc:   "link",
		field:    3,
		reportAs: "jobstats_link",
	},
	{
		inProc:   "unlink",
		field:    3,
		reportAs: "jobstats_unlink",
	},
	{
		inProc:   "mkdir",
		field:    3,
		reportAs: "jobstats_mkdir",
	},
	{
		inProc:   "rmdir",
		field:    3,
		reportAs: "jobstats_rmdir",
	},
	{
		inProc:   "rename",
		field:    3,
		reportAs: "jobstats_rename",
	},
	{
		inProc:   "getattr",
		field:    3,
		reportAs: "jobstats_getattr",
	},
	{
		inProc:   "setattr",
		field:    3,
		reportAs: "jobstats_setattr",
	},
	{
		inProc:   "getxattr",
		field:    3,
		reportAs: "jobstats_getxattr",
	},
	{
		inProc:   "setxattr",
		field:    3,
		reportAs: "jobstats_setxattr",
	},
	{
		inProc:   "statfs",
		field:    3,
		reportAs: "jobstats_statfs",
	},
	{
		inProc:   "sync",
		field:    3,
		reportAs: "jobstats_sync",
	},
	{
		inProc:   "samedir_rename",
		field:    3,
		reportAs: "jobstats_samedir_rename",
	},
	{
		inProc:   "crossdir_rename",
		field:    3,
		reportAs: "jobstats_crossdir_rename",
	},
}

// Parse a single line and create a field_name => field_value_string mapping
func ParseLine(line string, wanted_fields []*mapping) (map[string]string) {

	fields := map[string]string{}
	parts := strings.Fields(line)
	if strings.HasPrefix(line, "- job_id:") {
		fields["jobid"] = parts[2]
		return fields
	}

	for _, wanted := range wanted_fields {
		var key string
		if strings.TrimSuffix(parts[0], ":") == wanted.inProc {
			wanted_field := wanted.field
			// if not set, assume field[1]. Shouldn't be field[0], as
			// that's a string
			if wanted_field == 0 {
				wanted_field = 1
			}
			key = wanted.inProc
			if wanted.reportAs != "" {
				key = wanted.reportAs
			}
			fields[key] = strings.TrimSuffix((parts[wanted_field]), ",")
		}
	}

	return fields
}

// Parse each input line in each file, and build up a map with fields
// found and their values, contained by a map with Lustre target and
// JobId (if applicable) indicating the values to use for tagging
func (l *Lustre2) GetLustreProcStats(fileglob string, target_type string) error {
	files, err := filepath.Glob(fileglob)
	if err != nil {
		return err
	}

	for _, file := range files {
		var origin statSource
		var jobstats_file bool

		/* Turn /proc/fs/lustre/obdfilter/<ost_name>/stats and similar
		 * into just the object store target name
		 * Assumpion: the target name is always second to last,
		 * which is true in Lustre 2.1->2.8
		 */
		path := strings.Split(file, "/")
		origin.target = path[len(path)-2]
		jobstats_file = strings.HasSuffix(file, "job_stats")

		var wanted_fields []*mapping
		wanted_fields = l.wanted_maps[target_type][jobstats_file]

		var fields map[string]interface{}

		if jobstats_file == false {
			var ok bool
			fields, ok = l.allFields[origin]
			if !ok {
				fields = make(map[string]interface{})
				l.allFields[origin] = fields
			}
		}

		lines, err := internal.ReadLines(file)
		if err != nil {
			return err
		}

		for _, line := range lines {
			var data uint64
			var linefields map[string]string

			linefields = ParseLine(line, wanted_fields)

			if linefields["jobid"] != "" {
				origin.jobid = linefields["jobid"]
				fields = make(map[string]interface{})
				l.allFields[origin] = fields
			} else if len(linefields) != 0 {
				for key, value := range linefields {
					data, err = strconv.ParseUint(value, 10, 64)
					if err != nil {
						return err
						}
					fields[key] = data
				}
			}
		}
	}
	return nil
}

// SampleConfig returns sample configuration message
func (l *Lustre2) SampleConfig() string {
	return sampleConfig
}

// Description returns description of Lustre2 plugin
func (l *Lustre2) Description() string {
	return "Read metrics from local Lustre service on OST, MDS"
}

// Gather reads stats from all lustre targets
func (l *Lustre2) Gather(acc telegraf.Accumulator) error {
	l.allFields = make(map[statSource]map[string]interface{})

	l.wanted_maps = map[string]map[bool][]*mapping{
		"OST": map[bool][]*mapping{
			true:wanted_ost_jobstats_fields,
			false:wanted_ost_fields,
		},
		"MDT": map[bool][]*mapping{
			true:wanted_mdt_jobstats_fields,
			false:wanted_mds_fields,
		},
	}

	var ost_files []string
	var mdt_files []string

	/*
	 * This should probably either ONLY use the user-supplied files, if
	 * they specified any at all; or ONLY use the defaults, if none were
	 * supplied.  However, since that's an interface change, need to check
	 * what the telegraf policy is.
	 *
	 * Code below behaves like the plugin has in the past.
	 */
	ost_files = l.Ost_procfiles
	mdt_files = l.Mds_procfiles

	if len(l.Ost_procfiles) == 0 {
		ost_files = append(ost_files, "/proc/fs/lustre/obdfilter/*/stats")
		ost_files = append(ost_files, "/proc/fs/lustre/osd-ldiskfs/*/stats")
		ost_files = append(ost_files, "/proc/fs/lustre/osd-zfs/*/stats")
		ost_files = append(ost_files, "/proc/fs/lustre/obdfilter/*/job_stats")
	}

	if len(l.Mds_procfiles) == 0 {
		mdt_files = append(mdt_files, "/proc/fs/lustre/mdt/*/md_stats")
		mdt_files = append(mdt_files, "/proc/fs/lustre/mdt/*/job_stats")
	}

	for _, procfile := range ost_files {
		err := l.GetLustreProcStats(procfile, "OST")
		if err != nil {
			return err
		}
	}
	for _, procfile := range mdt_files {
		err := l.GetLustreProcStats(procfile, "MDT")
		if err != nil {
			return err
		}
	}

	for origin, fields := range l.allFields {
		tags := map[string]string{
			"name": origin.target,
		}
		if origin.jobid != "" {
			tags["jobid"] = origin.jobid
		}
		acc.AddFields("lustre2", fields, tags)
	}

	return nil
}

func init() {
	inputs.Add("lustre2", func() telegraf.Input {
		return &Lustre2{}
	})
}
