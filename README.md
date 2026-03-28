Release:
cargo build --path . --root ~/ && dotsync sync

systemctl --user stop prview.service
systemctl --user start prview.service
