# Octave AI Solution Engineer – Technical Assessment

## Overview

This repository contains the raw data and instructions for the Octave AI Solution Engineer technical assessment.

---

## The Task

You are provided with a collection of commercial insurance business submission emails (`.eml` files), which may or may not include attachments. Your task is to:

1. Extract relevant insurance data from these emails
2. Store the extracted data in a structured database
3. Provide a mechanism to interrogate that data

UX is an important factor in your solution.

---

## Raw Data

The `Emails/` and `Emails Round/` folders contain the submission emails to be used as input. The data has been anonymized — please treat it carefully and in accordance with your MNDA obligations.

---

## Data to Focus On

- Line(s) of business
- Limits requested
- Target pricing
- Exposure information (trucks, drivers, etc.)
- Loss information
- Broker information
- Insured information

---

## Requirements

- **Language:** Python or TypeScript
- **Database:** SQLite, PostgreSQL, or equivalent local DB
- **AI/LLM:** Use of an LLM is a key capability being evaluated — an Anthropic API key will be provided
- **Deployment:** Local machine, or private/secure public cloud

A brief explanation of your solution architecture should be included in your Solution-README.

---

## Submission

When complete, upload your solution to this repository. A walkthrough and live demo session will be scheduled, during which new data will be provided to run through the solution. The quality of the results and performance of the solution will be evaluated. 

A code review will be conducted prior to the demo session.

For API key issues, contact: cbrar@octavegroup.com
