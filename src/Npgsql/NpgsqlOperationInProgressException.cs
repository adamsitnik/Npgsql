using System;
using System.Linq;

namespace Npgsql
{
    /// <summary>
    /// Thrown when trying to use a connection that is already busy performing some other operation.
    /// Provides information on the already-executing operation to help with debugging.
    /// </summary>
    public sealed class NpgsqlOperationInProgressException : InvalidOperationException
    {
        internal NpgsqlOperationInProgressException(object currentlyExecuting)
            : base(currentlyExecuting is NpgsqlCommand cmd
                  ? "A command is already in progress: " + cmd.CommandText
                  : currentlyExecuting is NpgsqlCommandSet cmdset
                    ? "A command set is already in progress, first command: " + cmdset.Commands[0].CommandText
                    : $"An unknown operation is currently executing (type {currentlyExecuting.GetType().Name})")
        {
            switch (currentlyExecuting)
            {
            case NpgsqlCommand command:
                CommandInProgress = command;
                return;
            case NpgsqlCommandSet commandSet:
                CommandSetInProgress = commandSet;
                return;
            }
        }

        internal NpgsqlOperationInProgressException(ConnectorState state)
            : base($"The connection is already in state '{state}'")
        {
        }

        /// <summary>
        /// If the connection is busy with a command, this will contain a reference to that command.
        /// If it's a command set, this will contain null and <see cref="CommandSetInProgress"/> will contain the
        /// reference to that set. Otherwise, if the connection if busy with another type of operation (e.g. COPY),
        /// contains null.
        /// </summary>
        public NpgsqlCommand CommandInProgress { get; }

        /// <summary>
        /// If the connection is busy with a command set, this will contain a reference to that set.
        /// If it's a regular command , this will contain null and <see cref="CommandInProgress"/> will contain the
        /// reference to that command. Otherwise, if the connection if busy with another type of operation (e.g. COPY),
        /// contains null.
        /// </summary>
        public NpgsqlCommandSet CommandSetInProgress { get; }
    }
}
