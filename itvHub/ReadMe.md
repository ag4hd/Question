# How to run the code!

Please go to the root directory  i.e. itvHub.
Type the command **gradlew build**.
It will build the requirement jars for the project.

Run the scripts using the jars place in the folder **build/libs/itvHub.jar**.
or
You can run the code in the intellij using the **Run** provided on the top.

## InputFile

Following is the sample of input. The input file is located in the **src/main/resources/sample.csv**resources folder.

| start               | end                 |
|---------------------|---------------------|
| 2015-04-10 09:01:12 | 2015-04-10 11:18:20 |

# Output

Path of the output csv needs to be provided to write the output ot csv. Currently, output is shown on the screen but the output command is included to write the output.

| start               | end                 | MaxConcurrentPlays |
|---------------------|---------------------|--------------------|
| 2015-04-10 09:15:00 | 2015-04-10 11:18:20 | 4                  |
| 2015-04-10 17:30:00 | 2015-04-10 19:18:03 | 4                  |

#PowerPoint Presenation

Please refer the powerpoint attached with an email for the choosen approach to find solution and its pros/cons. Also the other approach is also mention in the powerpoint.
