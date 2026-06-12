# TASK

1. Do EXTENSIVE quality control checks on the changes that have just been made
    - We want to be sure that the changes that have been made prior to the introduction of the two agents did NOT cause regressions with the rest of the syntax-conversion.
    - Prior changes refer to the commit f7650a33886c41e03ea99227e6c824370b4f0d7f
    - Make use of these two agents to perform the extensive quality-control checks:
        - test-coverage-mapper
        - dba-sql-expert
            - Make those two agents collaborate. Make them work together to do the QA checks.

2. Identify all the bad paths that our syntax-conversion logic could have
    - Identify ALL, absolutely ALL the remaining edge cases that our syntax-conversion logic hasn't covered yet
    - Prioritize the bad paths. The worse they are, the higher the priority.
    - Make use of the following agents:
        - test-coverage-mapper
        - dba-sql-expert
            - test-coverage-mapper will be the one to lead the identification of ALL the bad paths and edge cases while the results will be provided to dba-sql-expert
            - dba-sql-expert will then recieve the results, and take action in order to resolve and harden against these edge cases and bad paths at the level of the syntax-conversion logic.

3. Absolutely NO HOTFIXING the converted postgres .sql output files.
    - ALL fixes must be made at the syntax-converter level.

4. For every iteration, run extensive testing against actual running DB containers.

Follow the above STRICTLY.