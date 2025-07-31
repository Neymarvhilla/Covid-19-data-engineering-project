-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS covid_processed
LOCATION "abfss://processed@covidreportingnesodl.dfs.core.windows.net/"
