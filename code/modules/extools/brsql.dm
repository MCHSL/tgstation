/datum/query_result
	var/affected = 0
	var/error = null
	var/list/rows = list()

/datum/query_result/New(data)
	if(!data)
		stack_trace("/datum/query_result created with null data!")
	var/list/json
	if(islist(data))
		json = data
	else
		json = json_decode(data)
		if(!islist(json))
			stack_trace("/datum/query_result created with invalid JSON!")
	switch(json["status"])
		if("ok")
			affected = "affected" in json ? json["affected"] : affected
			rows = "rows" in json ? json["rows"] : rows
		if("err")
			error = json["data"]
			log_world(error) // TODO: remove this once I'm done
