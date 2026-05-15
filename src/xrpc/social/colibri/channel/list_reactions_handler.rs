use futures::future::BoxFuture;
use rocket::serde::json::Json;
use rocket::{State, get};
use sea_orm::{DatabaseConnection, DbErr};
use serde::Serialize;

use crate::lib::at_uri::AtUri;
use crate::lib::reactions::{ReactionSummary, list_reactions_for_message};
use crate::lib::responses::{ErrorBody, ErrorResponse};

#[derive(Serialize, Debug)]
pub struct ReactionList {
    pub reactions: Vec<ReactionSummary>,
}

type ListReactionsFn =
    fn(DatabaseConnection, String) -> BoxFuture<'static, Result<Vec<ReactionSummary>, DbErr>>;

async fn list_reactions_with(
    message_uri: String,
    db: DatabaseConnection,
    list_reactions_fn: ListReactionsFn,
) -> Result<Json<ReactionList>, ErrorResponse> {
    let parsed = AtUri::parse(&message_uri).ok_or_else(|| ErrorResponse {
        body: Json(ErrorBody {
            error: String::from("InvalidRequest"),
            message: String::from("Invalid message AT-URI."),
        }),
    })?;

    let reactions = list_reactions_fn(db, parsed.rkey).await?;
    Ok(Json(ReactionList { reactions }))
}

fn list_reactions_boxed(
    db: DatabaseConnection,
    message_rkey: String,
) -> BoxFuture<'static, Result<Vec<ReactionSummary>, DbErr>> {
    Box::pin(async move { list_reactions_for_message(&db, &message_rkey).await })
}

#[get("/xrpc/social.colibri.channel.listReactions?<message>")]
/// Lists all reactions targeting a specific message, grouped by emoji.
pub async fn list_reactions(
    message: &str,
    db: &State<DatabaseConnection>,
) -> Result<Json<ReactionList>, ErrorResponse> {
    list_reactions_with(
        message.to_string(),
        db.inner().clone(),
        list_reactions_boxed,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::test_fixtures::mock_db;
    use rocket::tokio;

    #[tokio::test]
    async fn returns_reactions_from_summary_fn() {
        let db = mock_db();
        let result = list_reactions_with(
            String::from("at://did:plc:author/social.colibri.message/msg-1"),
            db,
            |_, rkey| {
                assert_eq!(rkey, "msg-1");
                Box::pin(async {
                    Ok(vec![ReactionSummary {
                        emoji: String::from("🦜"),
                        count: 2,
                        reactor_dids: vec![
                            String::from("did:plc:alice"),
                            String::from("did:plc:bob"),
                        ],
                    }])
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(result.reactions.len(), 1);
        assert_eq!(result.reactions[0].emoji, "🦜");
        assert_eq!(result.reactions[0].count, 2);
    }

    #[tokio::test]
    async fn rejects_invalid_message_uri() {
        let db = mock_db();
        let result = list_reactions_with(String::from("invalid"), db, |_, _| {
            Box::pin(async { panic!("should not call when uri is invalid") })
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "InvalidRequest"
        );
    }

    #[tokio::test]
    async fn propagates_db_error_as_upstream_error() {
        let db = mock_db();
        let result = list_reactions_with(
            String::from("at://did:plc:author/social.colibri.message/msg-1"),
            db,
            |_, _| Box::pin(async { Err(DbErr::Custom(String::from("boom"))) }),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().body.into_inner().error,
            "UpstreamError"
        );
    }
}
