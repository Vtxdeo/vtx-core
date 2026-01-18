use super::api;
use crate::runtime::context::StreamContext;

impl api::context::Host for StreamContext {
    async fn get_current_user(&mut self) -> Option<api::auth_types::CurrentUser> {
        self.current_user
            .as_ref()
            .map(|user| api::auth_types::CurrentUser {
                user_id: user.user_id.clone(),
                username: user.username.clone(),
                groups: user.groups.clone(),
            })
    }
}
