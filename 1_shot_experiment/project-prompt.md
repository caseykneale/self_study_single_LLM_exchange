```
I am going to give you directions to write me a simple command line application using the Rust programming language and the latest version of the bluer crate that you have in memory. For the command line use Rust's clap crate with the serde procedural macros for Serde. The coding style I want you to follow is to use 1 file, because the application should be pretty simple.

The application should do the following. 
1. It should listen for all bluetooth signals and acquire a unique identifier for them, not just a name, and their signal strength.
2. I need a data structure that compares these signals over time. For example a cache or hashmap like structure that makes a new entry per unique identifier. For each unique identifier key associate a collection ordered by time of arrival the time of the detection and it's signal strength.
3. I then want a mechanism, possibly using channels, which reviews this cache regularly. Given some time value (say 20 seconds) determine if any of the most recent signals are correlated or new. This is the real goal, I want to identify vehicles or people by their bluetooth signatures as they frequent a location.
4. When 'someone' (or a unique collection of bluetooth signals) new arrives I want the application to print a message to the console or to a log file, perhaps both, that gives a description of the event. It should include the most human readable bluetooth identifiers and any relevent metadata, perhaps when they were last observed? The log file should be more verbose and include any unique identifiers.

Please keep the code as simple as possible and the number of dependencies minimal. Create command line arguments for anything you feel should be variable. This needs to be correct to keep people safe. Thank you. 
```