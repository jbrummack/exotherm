pub mod database;
pub mod error;
pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{database::database::Database, error::SResult};

    use super::*;
    record!(Person {
        0 -> name: [name_index] String,
        1 -> password:[] String,
    });

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
    #[tokio::test]
    async fn insert() -> SResult<()> {
        let _guard = unsafe { foundationdb::boot() };
        let db = Database::new("testing").await?;
        let id = Uuid::new_v4();
        let person = Person {
            name: String::from("NameNameNameNameNamevName"),
            password: String::from("TestTestTestTestTest"),
        };

        db.transact(|transaction| async move {
            let eq = Person::name_index(Uuid::nil(), &String::from("NameNameNameNameNamevName"));
            let result = transaction
                .query_index(database::transaction::Query::Equal(eq), false)
                .await?;

            assert!(result.ids.len() > 0);

            println!("{result:#?}");
            Ok(())
        })
        .await?;
        db.transact(|transaction| {
            let person = &person;
            async move {
                transaction.put_value(person, id).await?;
                Ok(())
            }
        })
        .await?;
        db.transact(|transaction| async move {
            let person: Option<Person> = transaction.get_value(id).await?;
            println!("{person:#?}");
            Ok(())
        })
        .await?;
        db.transact(|transaction| async move {
            let person: Option<Person> = transaction.get_value(id).await?;
            if let Some(mut person) = person {
                person.password = String::from("neues passwort");
                transaction.put_value(&person, id).await?;
            }
            Ok(())
        })
        .await?;
        db.transact(|transaction| async move {
            let person: Option<Person> = transaction.get_value(id).await?;
            println!("{person:#?}");
            Ok(())
        })
        .await?;
        Ok(())
    }
}
