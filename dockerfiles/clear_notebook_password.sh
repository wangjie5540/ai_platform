#!/usr/bin/expect

spawn jupyter notebook password
expect {
        "Enter" {send "\r";exp_continue;}
        "Verify" { send "\r" }
}
interact