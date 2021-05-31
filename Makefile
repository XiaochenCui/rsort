test:
	cargo test -- --nocapture

fmt:
	rustup run nightly cargo fmt

gen:
	cargo test -- tests::generate --nocapture

cp:
	# rsync -zvha --exclude=".git/" --filter=':- .gitignore' ../rsort pi-root:/root/
	rsync -zvha --exclude=".git/" --filter=':- .gitignore' ../rsort magus-90:~/project/

install:
	cargo install --path .