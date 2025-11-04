# TODO: Modify Script to Check for Records Not Found Over 3-4 Days

- [x] Add new function `fetch_still_not_found_records(collection)` to fetch records created more than 3 days ago where email_found or phone_found is false.
- [x] Modify `analyze_records(records)` to include analysis for "still not found" records, adding counts by user, source, etc.
- [x] Update `print_report(analysis)` to include a new section for "Still Not Found After 3+ Days" records.
- [x] Update `main()` to call the new fetch function and include the new analysis in the report.
- [x] Test the script to ensure it runs without errors and produces the expected output.
