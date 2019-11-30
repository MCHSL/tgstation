SUBSYSTEM_DEF(dbcore)
	name = "Database"
	flags = SS_BACKGROUND
	init_order = INIT_ORDER_DBCORE
	var/const/FAILED_DB_CONNECTION_CUTOFF = 5
	var/failed_connection_timeout = 0

	var/schema_mismatch = 0
	var/db_minor = 0
	var/db_major = 0
	var/failed_connections = 0

	var/list/active_jobs = list() // list of "job id" = /datum/promise

	var/use_ffi = TRUE

/datum/controller/subsystem/dbcore/Initialize()
	//We send warnings to the admins during subsystem init, as the clients will be New'd and messages
	//will queue properly with goonchat
	switch(schema_mismatch)
		if(1)
			message_admins("Database schema ([db_major].[db_minor]) doesn't match the latest schema version ([DB_MAJOR_VERSION].[DB_MINOR_VERSION]), this may lead to undefined behaviour or errors")
		if(2)
			message_admins("Could not get schema version from database")

	return ..()

/datum/controller/subsystem/dbcore/fire()
	for(var/job in active_jobs)
		var/datum/promise/P = active_jobs[job]
		if(!P || !istype(P))
			continue
		var/list/ret = CallSync("job_status", job)
		if(islist(ret))
			switch(ret["status"])
				if("done")
					active_jobs -= job
					P.completed = TRUE
					P.result = ret["data"]
					P.__resolve_callback()
				if("invalid")
					active_jobs -= job
		if(MC_TICK_CHECK)
			return

/datum/controller/subsystem/dbcore/Shutdown()
	//This is as close as we can get to the true round end before Disconnect() without changing where it's called, defeating the reason this is a subsystem
	if(SSdbcore.Connect())
		QueryAsyncCB("UPDATE [format_table_name("round")] SET shutdown_datetime = Now(), end_state = :endgame WHERE id = :roundid", list(endgame = "[SSticker.end_state]", roundid = GLOB.round_id))

//nu
/datum/controller/subsystem/dbcore/can_vv_get(var_name)
	return var_name != NAMEOF(src, connection) && var_name != NAMEOF(src, active_queries) && var_name != NAMEOF(src, connectOperation) && ..()

/datum/controller/subsystem/dbcore/vv_edit_var(var_name, var_value)
	if(var_name == NAMEOF(src, connection) || var_name == NAMEOF(src, connectOperation))
		return FALSE
	return ..()

/datum/controller/subsystem/dbcore/proc/Connect()
	if(IsConnected())
		return TRUE

	if(failed_connection_timeout <= world.time) //it's been more than 5 seconds since we failed to connect, reset the counter
		failed_connections = 0

	if(failed_connections > FAILED_DB_CONNECTION_CUTOFF)	//If it failed to establish a connection more than 5 times in a row, don't bother attempting to connect for 5 seconds.
		failed_connection_timeout = world.time + 50
		return FALSE

	if(!CONFIG_GET(flag/sql_enabled))
		return FALSE

	var/user = CONFIG_GET(string/feedback_login)
	var/pass = CONFIG_GET(string/feedback_password)
	var/db = CONFIG_GET(string/feedback_database)
	var/address = CONFIG_GET(string/address)
	var/port = CONFIG_GET(number/port)
	var/timeout = CONFIG_GET(number/query_timeout)
	var/max_threads = CONFIG_GET(number/thread_limit)
	use_ffi = fexists(EXTOOLS) && fexists(BRSQL) && CONFIG_GET(flag/use_extools)
	if(use_ffi)
		try
			if(extools_initialize())
				if(!tffi_initialize())
					use_ffi = FALSE
			else
				use_ffi = FALSE
		catch(var/exception/e)
			use_ffi = FALSE
			stack_trace("SSdbcore failed to load byond-extools! [e.file]:[e.line]")
	var/list/status = CallSync("create_pool", "[address]", "[port]", "[user]", "[pass]", "[db]", "[timeout]", "[max_threads]")
	if(status["status"] == "ok")
		return TRUE
	failed_connections++
	log_sql("Failed to connect to SQL: [status["data"]]")
	return FALSE

/datum/controller/subsystem/dbcore/proc/CheckSchemaVersion()
	if(CONFIG_GET(flag/sql_enabled))
		if(Connect())
			log_world("Database connection established.")
			QueryAsyncCB("SELECT major, minor FROM [format_table_name("schema_revision")] ORDER BY date DESC LIMIT 1", CALLBACK(src, .proc/SchemaVersionCallback))
		else
			log_sql("Your server failed to establish a connection with the database.")
	else
		log_sql("Database is not enabled in configuration.")

/datum/controller/subsystem/dbcore/proc/SchemaVersionCallback(result)
	var/datum/query_result/query = new(result)
	if(!LAZYLEN(query.rows))
		schema_mismatch = 2 //flag admin message about no schema version
		log_sql("Could not get schema version from database")
		return
	var/db_major = query.rows["major"]
	var/db_minor = query.rows["minor"]
	if(db_major != DB_MAJOR_VERSION || db_minor != DB_MINOR_VERSION)
		schema_mismatch = 1 // flag admin message about mismatch
		log_sql("Database schema ([db_major].[db_minor]) doesn't match the latest schema version ([DB_MAJOR_VERSION].[DB_MINOR_VERSION]), this may lead to undefined behaviour or errors")

/datum/controller/subsystem/dbcore/proc/SetRoundID()
	if(!Connect())
		return
	QuerySync("INSERT INTO [format_table_name("round")] (initialize_datetime, server_ip, server_port) VALUES (Now(), INET_ATON(IF(:ip LIKE '', '0', :ip)), :port)", list(ip = "[world.internet_address]", port = "[world.port]"))
	var/datum/query_result/QR = QuerySync("SELECT LAST_INSERT_ID()")
	world.log << json_encode(QR.rows)
	//GLOB.round_id = blahblah

/datum/controller/subsystem/dbcore/proc/SetRoundStart()
	if(!Connect())
		return
	QueryAsync("UPDATE [format_table_name("round")] SET start_datetime = Now() WHERE id = :roundid", list(roundid = GLOB.round_id))

/datum/controller/subsystem/dbcore/proc/SetRoundEnd()
	if(!Connect())
		return
	QueryAsync("UPDATE [format_table_name("round")] SET end_datetime = Now(), game_mode_result = :result, station_name = :stationname WHERE id = :roundid", list(result = "[SSticker.mode_result]", stationname = "[station_name()]", roundid = GLOB.round_id))

/datum/controller/subsystem/dbcore/proc/CallSync()
	var/list/arguments = args.Copy()
	var/method = arguments[1]
	if(use_ffi)
		arguments.Insert(1, BRSQL)
		var/datum/promise/P = call_async(arglist(args))
		var/data = P.resolve()
		if(!data)
			stack_trace("BRSQL FFI call to '[method]' didn't return valid data!")
			return
		var/list/json = json_decode(data)
		if(!LAZYLEN(json))
			stack_trace("BRSQL FFI call to '[method]' didn't return valid JSON!")
			return
		return json
	else
		arguments.Cut(1, 2)
		var/data = call(BRSQL, method)(arglist(arguments))
		if(!data)
			stack_trace("BRSQL call to '[method]' didn't return valid data!")
			return
		var/list/json = json_decode(data)
		if(!LAZYLEN(json))
			stack_trace("BRSQL call to '[method]' didn't return valid JSON!")
			return
		return json

// CallAsyncCB(method, callback, ...)
/datum/controller/subsystem/dbcore/proc/CallAsyncCB()
	set waitfor = 0
	var/list/arguments = args.Copy()
	var/method = arguments[1]
	var/datum/callback/CB = arguments[2]
	arguments.Cut(1, 3)
	if(use_ffi)
		arguments.Insert(1, BRSQL, method, CB)
		call_cb(arglist(arguments))
	else // lmao sucks to be you
		spawn(0)
			var/result = call(BRSQL, method)(arglist(arguments))
			CB.InvokeAsync(result)

/datum/controller/subsystem/dbcore/proc/CallAsync()
	var/list/arguments = args.Copy()
	var/method = arguments[1]
	arguments.Cut(1, 2)
	if(use_ffi)
		arguments.Insert(1, BRSQL, method)
		return call_async(arglist(arguments))
	else // fake promise time!
		var/datum/promise/P = new
		spawn(0)
			var/result = call(BRSQL, method)(arglist(arguments))
			P.completed = TRUE
			P.result = result
		return P

/datum/controller/subsystem/dbcore/proc/IsConnected()
	. = FALSE
	if(!CONFIG_GET(flag/sql_enabled))
		return
	var/list/status = CallSync("pool_status")
	return status["status"] == "online"

/datum/controller/subsystem/dbcore/proc/QuerySync(query, list/params, warn = FALSE)
	LAZYINITLIST(params)
	var/list/json = CallSync("query", query, json_encode(params))
	var/datum/query_result/QR = new(json)
	if(warn && QR.error)
		to_chat(usr, "<span class='danger'>A SQL error occurred during this operation, check the server logs.</span>")
	return QR

/datum/controller/subsystem/dbcore/proc/QueryAsyncCB(query, datum/callback/CB, list/params)
	LAZYINITLIST(params)
	if(use_ffi)
		CallAsyncCB("query", query, CB, json_encode(params))
	else
		var/job_id = call(BRSQL, "query_fb")(query, json_encode(params))
		if(job_id)
			var/datum/promise/P = new
			P.callback = CB
			active_jobs[job_id] = P

/datum/controller/subsystem/dbcore/proc/QueryAsync(query, list/params)
	LAZYINITLIST(params)
	return CallAsync("query", query, json_encode(params))

/datum/controller/subsystem/dbcore/proc/QuerySelect(list/querys) // querys is ["name"] = list("query", params)
	var/list/constructed = list()
	var/list/toilet = list()
	var/list/results = list()
	if(!islist(querys))
		CRASH("Non-list passed to QuerySelect")
	for (var/id in querys)
		var/thing = querys[id]
		var/list/query
		if(istext(thing))
			query = list("[thing]", list())
		else if(islist(thing))
			query = thing
			if(length(query) > 2)
				query.Cut(3)
			else if(length(query) == 1)
				query += list(list())
		constructed[id] = query

	for (var/id in constructed)
		toilet[id] = QueryAsync(constructed[id][1], constructed[id][2])

	for(var/poop in toilet) // i can't name vars
		var/water = toilet[poop]
		if(istype(water, /datum/promise))
			var/datum/promise/promise = water
			if(promise.completed)
				toilet -= poop
				results[poop] = new /datum/query_result(promise.result)
				qdel(promise)
		else if(istext(water))
			var/list/ret = CallSync("job_status", poop)
			if(islist(ret))
				switch(ret["status"])
					if("done")
						results[poop] = new /datum/query_result(ret["data"])
						toilet -= poop
					if("invalid")
						results[poop] = null
						toilet -= poop
	return results



/*
Takes a list of rows (each row being an associated list of column => value) and inserts them via a single mass query.
Rows missing columns present in other rows will resolve to SQL NULL
You are expected to do your own escaping of the data, and expected to provide your own quotes for strings.
The duplicate_key arg can be true to automatically generate this part of the query
	or set to a string that is appended to the end of the query
Ignore_errors instructes mysql to continue inserting rows if some of them have errors.
	 the erroneous row(s) aren't inserted and there isn't really any way to know why or why errored
Delayed insert mode was removed in mysql 7 and only works with MyISAM type tables,
	It was included because it is still supported in mariadb.
	It does not work with duplicate_key and the mysql server ignores it in those cases
*/
/datum/controller/subsystem/dbcore/proc/MassInsert(table, list/rows, list/replacements, duplicate_key = FALSE, ignore_errors = FALSE, delayed = FALSE, warn = FALSE, async = TRUE)
	if (!table || !rows || !istype(rows))
		return
	var/list/columns = list()
	var/column_len
	for (var/i = 1 to rows.len)
		var/list/row = rows[i]
		if(column_len && row.len != column_len)
			row.len = column_len
		column_len = row.len
		for(var/column in row)
			columns |= column


	if (duplicate_key == TRUE)
		var/list/column_list = list()
		for (var/column in columns)
			column_list += "[column] = VALUES([column])"
		duplicate_key = "ON DUPLICATE KEY UPDATE [column_list.Join(", ")]\n"
	else if (duplicate_key == FALSE)
		duplicate_key = null

	if (ignore_errors)
		ignore_errors = " IGNORE"
	else
		ignore_errors = null

	if (delayed)
		delayed = " DELAYED"
	else
		delayed = null

	var/list/sqlrowlist = list()
	var/list/values = list()
	var/i = 1
	for(var/list/row in rows)
		var/list/p_row = list()
		for(var/column in row)
			var/value = row[column]
			var/key = "[i]_[column]"
			if(istext(replacements[column]))
				p_row += replacetext(replacements[column], "%P%", ":[i]_")
			else
				p_row += ":[key]"
			values[key] = value
		sqlrowlist += "([p_row.Join(", ")])"
		i++

	sqlrowlist = "	[sqlrowlist.Join(",\n	")]"
	return QueryAsync("INSERT[delayed][ignore_errors] INTO [table]\n([columns.Join(", ")])\nVALUES\n[sqlrowlist]\n[duplicate_key]")
