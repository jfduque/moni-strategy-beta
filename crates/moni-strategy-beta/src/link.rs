use crate::gamma::BinaryCryptoMarket;
use crate::pricing::{Direction, Opportunity};
use anyhow::{Context, Result};
use moni_proto::link::v1 as pb;
use moni_proto::link::v1::engine_link_client::EngineLinkClient;
use tonic::Code;
use tonic::transport::Channel;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    Accepted,
    Duplicate,
    Rejected(String),
    Retriable(String),
}

#[derive(Clone)]
pub struct Submitter {
    client: EngineLinkClient<Channel>,
    automatic_submission_supported: bool,
}

impl Submitter {
    pub async fn connect(endpoint: String) -> Result<Self> {
        let channel = Channel::from_shared(endpoint)
            .context("invalid EngineLink endpoint")?
            .connect()
            .await
            .context("connecting EngineLink")?;
        let mut client = EngineLinkClient::new(channel);
        let capabilities = client
            .get_engine_info(pb::GetEngineInfoRequest {})
            .await
            .map(|response| response.into_inner().capabilities)
            .unwrap_or_default();
        let automatic_submission_supported = supports_automatic_submission(&capabilities);
        Ok(Self {
            client,
            automatic_submission_supported,
        })
    }

    pub async fn submit(
        &mut self,
        request: pb::SubmitCompleteSetSignalRequest,
    ) -> Result<SubmitOutcome> {
        if !self.automatic_submission_supported {
            return Ok(SubmitOutcome::Rejected(
                "engine lacks close-recovery/reconciliation capabilities".to_owned(),
            ));
        }
        match self.client.submit_complete_set_signal(request).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.duplicate {
                    Ok(SubmitOutcome::Duplicate)
                } else if response.accepted {
                    Ok(SubmitOutcome::Accepted)
                } else {
                    Ok(SubmitOutcome::Rejected(response.reason))
                }
            }
            Err(status) if matches!(status.code(), Code::Unavailable | Code::ResourceExhausted) => {
                Ok(SubmitOutcome::Retriable(format!(
                    "{:?}: {}",
                    status.code(),
                    status.message()
                )))
            }
            Err(status) => Err(status).context("submitting complete-set signal"),
        }
    }
}

fn supports_automatic_submission(capabilities: &[String]) -> bool {
    ["close_recovery_v1", "complete_set_reconciliation_v1"]
        .iter()
        .all(|required| capabilities.iter().any(|value| value == required))
}

#[allow(clippy::too_many_arguments)]
pub fn request(
    signal_id: String,
    strategy_id: &str,
    source: &str,
    market: &BinaryCryptoMarket,
    opportunity: &Opportunity,
    token_a_book_observed_at_ms: i64,
    token_b_book_observed_at_ms: i64,
    observed_at_ms: i64,
    recovery_enabled: bool,
    close_lead_ms: i64,
    max_loss: rust_decimal::Decimal,
) -> pb::SubmitCompleteSetSignalRequest {
    let (direction, max_pair_cost, min_pair_proceeds) = match opportunity.direction {
        Direction::BuyMerge => (
            pb::CompleteSetDirection::BuyMerge as i32,
            opportunity.pair_value.to_string(),
            String::new(),
        ),
        Direction::SplitSell => (
            pb::CompleteSetDirection::SplitSell as i32,
            String::new(),
            opportunity.pair_value.to_string(),
        ),
    };
    pb::SubmitCompleteSetSignalRequest {
        signal_id,
        strategy_id: strategy_id.to_owned(),
        source: source.to_owned(),
        market_id: market.market_id.clone(),
        condition_id: market.condition_id.clone(),
        token_a_id: market.outcomes[0].token_id.clone(),
        token_b_id: market.outcomes[1].token_id.clone(),
        direction,
        quantity: opportunity.quantity.to_string(),
        max_pair_cost,
        min_pair_proceeds,
        expected_profit: opportunity.net_profit.to_string(),
        expected_return_bps: opportunity.return_bps.to_string(),
        token_a_book_observed_at_ms,
        token_b_book_observed_at_ms,
        observed_at_ms,
        close_recovery: recovery_enabled.then(|| pb::CloseRecoveryPolicy {
            lead_time_ms: close_lead_ms,
            max_loss: max_loss.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gamma::OutcomeToken;
    use rust_decimal::Decimal;

    #[test]
    fn automatic_submission_requires_both_engine_capabilities() {
        assert!(!supports_automatic_submission(&[]));
        assert!(!supports_automatic_submission(&[
            "close_recovery_v1".to_owned()
        ]));
        assert!(supports_automatic_submission(&[
            "complete_set_reconciliation_v1".to_owned(),
            "close_recovery_v1".to_owned(),
        ]));
    }

    #[test]
    fn request_is_tenant_agnostic_and_direction_specific() {
        let market = BinaryCryptoMarket {
            market_id: "m".to_owned(),
            condition_id: "c".to_owned(),
            question: "q".to_owned(),
            rules: "r".to_owned(),
            underlying: "BTC".to_owned(),
            outcomes: [
                OutcomeToken {
                    label: "Yes".to_owned(),
                    token_id: "a".to_owned(),
                },
                OutcomeToken {
                    label: "No".to_owned(),
                    token_id: "b".to_owned(),
                },
            ],
            start_time_ms: None,
            end_time_ms: 2,
            tick_size: Decimal::new(1, 2),
            min_order_size: Decimal::ONE,
            neg_risk: false,
            gamma_fee_rate: Some(Decimal::new(7, 2)),
            gamma_fee_exponent: Some(1),
            gamma_fee_taker_only: Some(true),
            active: true,
            accepting_orders: true,
        };
        let opportunity = Opportunity {
            direction: Direction::BuyMerge,
            quantity: Decimal::ONE,
            leg_a_gross: Decimal::new(4, 1),
            leg_b_gross: Decimal::new(4, 1),
            fees: Decimal::ZERO,
            reserves: Decimal::ZERO,
            pair_value: Decimal::new(8, 1),
            capital: Decimal::new(8, 1),
            net_profit: Decimal::new(2, 1),
            return_bps: Decimal::from(2_500),
        };
        let request = request(
            "id".to_owned(),
            "strategy-beta",
            "beta",
            &market,
            &opportunity,
            1,
            1,
            1,
            true,
            30_000,
            Decimal::new(50, 2),
        );
        assert_eq!(request.max_pair_cost, "0.8");
        assert!(request.min_pair_proceeds.is_empty());
        assert_eq!(request.close_recovery.unwrap().lead_time_ms, 30_000);
    }
}
