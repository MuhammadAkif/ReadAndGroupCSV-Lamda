import * as handler from '../csv_converter';

test('Group, convert and upload csv files', async () => {
  const event = 'event';
  const context = 'context';

  const response = await handler.csvConverter(event, context);
  expect(response.statusCode).toEqual(200);
  expect(response.message).toEqual("Files uploaded");
});
