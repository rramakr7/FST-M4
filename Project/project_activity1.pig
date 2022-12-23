-- Load the input
inputFile = LOAD 'hdfs:///user/ramya/InputData' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Filter out first 2 lines
ranked = RANK inputFile;
ranked_lines = FILTER ranked BY (rank_inputFile > 2);

-- Group lines by name
grpd = GROUP ranked_lines BY name;

-- Count the number of lines by each character and order in descending order
total_count = FOREACH grpd GENERATE $0 as name, COUNT($1) AS no_of_lines;
result = ORDER total_count BY no_of_lines DESC;

-- Store the results in hdfs
STORE result INTO 'hdfs:///user/ramya/PigProjectOutput';
