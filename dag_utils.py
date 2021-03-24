#!/bin/python
# -*- coding: utf-8 -*-
# @File  : dw-dev-hour.py
# @Author: wangms
# @Date  : 2021/01/15
# @Brief: 简述报表功能
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


class OperatorFactory:
    def __init__(self, dag, op_kwargs=None):
        self.dag = dag
        self.op_kwargs = op_kwargs
        self.default_retries = 2

    def create_py(self, name, func, **kwargs):
        kwargs["retries"] = kwargs.get("retries", self.default_retries)
        return PythonOperator(
            task_id=name,
            python_callable=func,
            op_kwargs=self.op_kwargs,
            dag=self.dag,
            **kwargs
        )

    def create_dummy(self, name, **kwargs):
        kwargs["retries"] = kwargs.get("retries", self.default_retries)
        return DummyOperator(
            task_id=name,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=self.dag,
            **kwargs
        )

    def create_sh(self, name, command, **kwargs):
        kwargs["retries"] = kwargs.get("retries", self.default_retries)
        return BashOperator(
            task_id=name,
            bash_command=command,
            dag=self.dag,
            **kwargs
        )


class OperatorLink:
    def __init__(self, op_factory: OperatorFactory):
        self.start_op = None
        self.last_op = []
        self.op_factory = op_factory

    def to_py(self, name, func, **kwargs):
        operator = self.op_factory.create_py(name, func, **kwargs)
        return self._to(operator)

    def to_dummy(self, name, **kwargs):
        operator = self.op_factory.create_dummy(name, **kwargs)
        return self._to(operator)

    def to_sh(self, name, command, **kwargs):
        operator = self.op_factory.create_sh(name, command, **kwargs)
        return self._to(operator)

    def _to(self, operator):
        if not self.start_op:
            self.start_op = operator
        else:
            self.last_op >> operator
        self.last_op = operator
        return self

    def to_operators(self, *operators):
        assert self.start_op, "start_operator is None"
        for op in operators:
            self.last_op >> op
        return self
