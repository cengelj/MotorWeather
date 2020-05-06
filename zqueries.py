typecodes = {
    "WT01": "Fog, ice fog, or freezing fog (may include heavy fog)",
    "WT02": "Heavy fog or heavy freezing fog (not always distinguished from fog)",
    "WT03": "Thunder",
    "WT04": "Ice pellets, sleet, snow pellets, or small hail",
    "WT06": "Glaze or rime",
    "WT08": "Smoke or haze",
    "WT11": "High or damaging winds",
    "WT13": "Mist",
    "WT14": "Drizzle",
    "WT16": "Rain (may include freezing rain, drizzle, and freezing drizzle)",
    "WT18": "Snow, snow pellets, snow grains, or ice crystals",
    "WT19": "Unknown source of precipitation",
    "WT22": "Ice fog or freezing fog"
}

# --------------------------------------------------------------------------
# WEATHER CONDITIONS ON GIVEN DAY
# --------------------------------------------------------------------------

date = input("Enter a date (YYYY/MM/DD) to gather the weather data:")

query = """
SELECT maxtemp, mintemp, precip, snow, snowdepth, avgwind
FROM Temperature, Precipitation, Wind
WHERE date = %s
"""

query2 = """
SELECT * FROM Wtypes
WHERE date = %s
"""

results = execute_query(query, date)
# NEED ACCESS TO CURSOR TO GET COLUMN NAMES
codes = [desc[0] for desc in cursor.description]
codevalues = execute_query(query2, date)

codeinfo = list(zip(codes, codevalues))


print("High Temperature (F) {}", results[0])
print("Low Temperature  (F) {}", results[1])
print("Total precipitation  {}", results[2])
print("Total snow ......... {}", results[3])
print("Snow depth ......... {}", results[4])
print("Average Wind ....... {}", results[5])

print("\nWeather events:")

for code in codeinfo:
    if (code[1]):
        print("- {}".format(typecodes[code[0]]))






# --------------------------------------------------------------------------
# MOST COMMON WEATHER CONDITIONS
# --------------------------------------------------------------------------

def typesort(t):
    return t[1]

query = """
SELECT COUNT(%s) FROM Wtypes
"""

results = []

for typecode in typecodes.keys():
    typecount = execute_query(query, (typecode))[0]
    results.append((typecode, typecount))

results.sort(key=typesort)

print("Most common weather conditions (descending):")

currentrank = 1
for typecode in results:
    print("{}. {}: {} occurrences", currentrank, typecodes[typecode[0]], typecode[1])
    currentrank += 1