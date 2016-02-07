import ko from 'knockout';
import { bucketList } from 'model';

export default [
    {
        validator: ko.validation.rules.required.validator,
        message: 'Please enter a name for the bucket'
    },
    {
        validator: ko.validation.rules.minLength.validator,
        message: 'Name must be between 3 and 63 characters',
        params: 3
    },
    {
        validator: ko.validation.rules.maxLength.validator,
        message: 'Name must be between 3 and 63 characters',
        params: 63
    },
    { 
        validator: name => !name.includes('..'),
        message: 'Name cannot contain two adjacent periods'
    },
    {
        validator: name => !name.includes('.-') && !name.includes('-.'),
        message: 'Name cannot contain dashes next to periods'
    },
    {
        validator: name => !/\s/.test(name),
        message: 'Name cannot contain a whitespace'
    },
    {
        validator: name => /^[a-z0-9].*[a-z0-9]$/.test(name),
        message: 'Name must start and end with a letter or number'
    },
    {
        validator: name => !/^\d+\.\d+\.\d+\.\d+$/.test(name),
        message: 'Name cannot be in the form of an IP address'
    },
    {
        validator: name => /^[a-z0-9.-]*$/.test(name),
        message: 'Name can contain only lowercase letters, numbers, dashes and dots'
    },    
    {
        validator: name => bucketList().every(bucket => bucket.name !== name),
        message: 'A bucket with the same name already exist'
    }
];