# RDeeBee

This system is inspired by Martin Kleppman's arguments that Event Sourcing system and Databases are rather two sides of the same coin. It's an area that fascinates me and I wanted to work on the internals of a system like this as far as possible. This desire gave birth to `rdeebee`.

The overall idea behind this project is to implement a distributed event database that also provides `change data capture`. Something that would combine the command and query (CQRS designs) side databases/message buses a bit.

The overall goal is to learn about design and design tradeoffs by making them.