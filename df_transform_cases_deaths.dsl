source(output(
		country as string,
		country_code as string,
		continent as string,
		population as string,
		indicator as string,
		daily_count as integer,
		date as string,
		rate_14_day as string,
		source as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> CasesAndDeathsSource
source(output(
		country as string,
		country_code_2_digit as string,
		country_code_3_digit as string,
		continent as string,
		population as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> CountryLookUp
CasesAndDeathsSource filter(continent == "Europe" && not(isNull(country_code))) ~> FilterEuropeOnly
FilterEuropeOnly select(mapColumn(
		country,
		country_code,
		population,
		indicator,
		daily_count,
		source,
		each(match(name=="date"),
			"reported"+"_date" = $$)
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectOnlyRequiredFields
SelectOnlyRequiredFields pivot(groupBy(country,
		country_code,
		population,
		source,
		reported_date),
	pivotBy(indicator, ['confirmed cases', 'deaths']),
	count = sum(daily_count),
	columnNaming: '$V_$N',
	lateral: true) ~> PivotCounts
PivotCounts, CountryLookUp lookup(PivotCounts@country == CountryLookUp@country,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> LookupCountry
LookupCountry select(mapColumn(
		country = PivotCounts@country,
		country_code_2_digit,
		country_code_3_digit,
		population = PivotCounts@population,
		cases_count = {confirmed cases_count},
		deaths_count,
		reported_date,
		source
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> SelectForSink
SelectForSink sink(allowSchemaDrift: true,
	validateSchema: false,
	partitionFileNames:['cases_and_deaths.csv'],
	truncate: true,
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> CasesAndDeathsSink