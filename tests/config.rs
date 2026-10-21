use clap::Parser;
use hft_test_task::config::Cli;
#[test]
fn validates_cli_values() {
    let default = Cli::try_parse_from(["hft"]).unwrap();
    assert!(default.robot_config().is_ok());
    for args in [
        ["--max-age-ms", "0"],
        ["--max-age-ms", "3001"],
        ["--max-skew-ms", "501"],
        ["--min-depth-qty", "0"],
        ["--threshold-bps", "0"],
        ["--threshold-bps", "10001"],
    ] {
        let cli = Cli::try_parse_from(["hft", args[0], args[1]]).unwrap();
        assert!(cli.robot_config().is_err());
    }
}
