# How to run

```bash
go run cmd/main.go -query='{"page_id": "603355693096453"}' -path="/Users/m13/Downloads/backup_2023-01-16-2200_backup_2023-01-16-2200_rs-0-2_chatfuel_user_state.bson.gz" -fields="_id,page_id,variables,fb_user_info_revised_date,ig_user_info_changed_date,created_date"
```


go run cmd/main.go -query='{"created_date": {"$gt": {"$date": "2005-01-10T00:00:00.000Z"}}}' -path="/Users/m13/Downloads/backup_2023-01-16-2200_backup_2023-01-16-2200_rs-0-2_chatfuel_user_state.bson.gz" -fields="_id,created_date"